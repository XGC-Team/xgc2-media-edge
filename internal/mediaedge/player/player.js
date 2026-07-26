(() => {
  "use strict";

  const video = document.getElementById("video");
  const message = document.getElementById("message");
  const state = document.getElementById("state");
  const reconnect = document.getElementById("reconnect");
  const sourceId = document.body.dataset.sourceId;

  let peer = null;
  let sessionId = "";
  let generation = 0;

  function setState(label, value, detail = "") {
    state.textContent = label;
    state.dataset.state = value;
    message.textContent = detail;
  }

  function waitForICEGathering(connection) {
    if (connection.iceGatheringState === "complete") {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      const timeout = window.setTimeout(() => {
        connection.removeEventListener("icegatheringstatechange", changed);
        reject(new Error("browser ICE gathering timed out"));
      }, 15000);
      function changed() {
        if (connection.iceGatheringState !== "complete") {
          return;
        }
        window.clearTimeout(timeout);
        connection.removeEventListener("icegatheringstatechange", changed);
        resolve();
      }
      connection.addEventListener("icegatheringstatechange", changed);
    });
  }

  function closeSession() {
    generation += 1;
    const closingPeer = peer;
    const closingSessionId = sessionId;
    peer = null;
    sessionId = "";
    video.srcObject = null;
    if (closingPeer) {
      closingPeer.close();
    }
    if (closingSessionId) {
      void fetch(`/api/v1/sessions/${encodeURIComponent(closingSessionId)}`, {
        method: "DELETE",
        credentials: "omit",
        keepalive: true,
      });
    }
  }

  async function connect() {
    closeSession();
    const currentGeneration = generation;
    reconnect.disabled = true;
    setState("Connecting", "starting", "Negotiating a direct WebRTC session…");

    const connection = new RTCPeerConnection();
    peer = connection;
    connection.addTransceiver("video", { direction: "recvonly" });
    connection.addEventListener("track", (event) => {
      if (currentGeneration !== generation) {
        return;
      }
      video.srcObject = event.streams[0] || new MediaStream([event.track]);
      message.textContent = "";
      void video.play().catch(() => {});
    });
    connection.addEventListener("connectionstatechange", () => {
      if (currentGeneration !== generation) {
        return;
      }
      if (connection.connectionState === "connected") {
        reconnect.disabled = false;
        setState("Connected", "connected");
      } else if (connection.connectionState === "failed" ||
          connection.connectionState === "disconnected") {
        reconnect.disabled = false;
        setState("Connection lost", "error", "The direct WebRTC connection stopped. Reconnect to try again.");
      }
    });

    try {
      const offer = await connection.createOffer();
      await connection.setLocalDescription(offer);
      await waitForICEGathering(connection);
      if (currentGeneration !== generation || !connection.localDescription) {
        return;
      }
      const response = await fetch(
        `/api/v1/sources/${encodeURIComponent(sourceId)}/sessions`,
        {
          method: "POST",
          credentials: "omit",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ sdp: connection.localDescription.sdp }),
        },
      );
      const answer = await response.json();
      if (!response.ok) {
        throw new Error(answer.error || `session request failed (${response.status})`);
      }
      if (currentGeneration !== generation) {
        void fetch(`/api/v1/sessions/${encodeURIComponent(answer.sessionId)}`, {
          method: "DELETE",
          credentials: "omit",
          keepalive: true,
        });
        return;
      }
      sessionId = answer.sessionId;
      await connection.setRemoteDescription({ type: "answer", sdp: answer.sdp });
    } catch (error) {
      if (currentGeneration !== generation) {
        return;
      }
      closeSession();
      reconnect.disabled = false;
      setState("Error", "error", error instanceof Error ? error.message : "Unable to open the video session.");
    }
  }

  reconnect.addEventListener("click", () => {
    void connect();
  });
  window.addEventListener("pagehide", closeSession);
  void connect();
})();
