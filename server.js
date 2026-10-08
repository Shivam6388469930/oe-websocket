const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 4001;

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
  });

  res.end(
    JSON.stringify({
      success: true,
      service: "Survey Realtime Server",
    })
  );
});

const wss = new WebSocket.Server({
  server,
});

// ============================================================
// DQA LOCKS
// ============================================================

const oeLocks = new Map();

const LOCK_TIMEOUT = 30 * 60 * 1000;

// ============================================================
// HELPERS
// ============================================================

function now() {
  return Date.now();
}

function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcast(payload) {
  const message = JSON.stringify(payload);

  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

function broadcastToRole(role, payload) {
  const message = JSON.stringify(payload);

  wss.clients.forEach((client) => {
    if (
      client.readyState === WebSocket.OPEN &&
      client.user?.role === role
    ) {
      client.send(message);
    }
  });
}

function broadcastToUser(memberName, payload) {
  const message = JSON.stringify(payload);

  wss.clients.forEach((client) => {
    if (
      client.readyState === WebSocket.OPEN &&
      String(client.user?.memberName || "").toLowerCase() ===
        String(memberName || "").toLowerCase()
    ) {
      client.send(message);
    }
  });
}

function isExpired(lock) {
  return !lock || lock.expiresAt <= now();
}

// ============================================================
// CLEAN EXPIRED LOCKS
// ============================================================

function cleanupExpiredLocks() {
  const current = now();

  for (const [oeId, lock] of oeLocks.entries()) {
    if (lock.expiresAt <= current) {
      oeLocks.delete(oeId);

      broadcast({
        type: "oe-unlocked",
        oeId,
        reason: "expired",
      });
    }
  }
}

setInterval(cleanupExpiredLocks, 10_000);

// ============================================================
// CONNECTION
// ============================================================

wss.on("connection", (ws) => {
  console.log("Realtime client connected");

  ws.user = {
    role: "",
    memberName: "",
  };

  // Send current locks to newly connected client.
  send(ws, {
    type: "current-locks",
    locks: Array.from(oeLocks.values()),
  });

  // ==========================================================
  // MESSAGE
  // ==========================================================

  ws.on("message", (raw) => {
    try {
      const message = JSON.parse(raw.toString());

      const type = message.type;

      // ======================================================
      // IDENTIFY USER
      // ======================================================

      if (type === "identify") {
        ws.user = {
          role: String(message.role || "").trim().toLowerCase(),
          memberName: String(message.memberName || "").trim(),
        };

        send(ws, {
          type: "identified",
          role: ws.user.role,
          memberName: ws.user.memberName,
        });

        return;
      }

      // ======================================================
      // DQA START WORK
      // ======================================================

      if (type === "oe-start") {
        const oeId = String(message.oeId || "").trim();
        const dqaName = String(message.dqaName || "").trim();

        if (!oeId || !dqaName) {
          send(ws, {
            type: "oe-start-result",
            success: false,
            message: "OE ID and DQA name are required.",
          });

          return;
        }

        const existing = oeLocks.get(oeId);

        if (existing && isExpired(existing)) {
          oeLocks.delete(oeId);
        }

        const current = oeLocks.get(oeId);

        // Someone else already owns it.
        if (
          current &&
          current.dqaName.toLowerCase() !== dqaName.toLowerCase()
        ) {
          send(ws, {
            type: "oe-start-result",
            success: false,
            oeId,
            lockedBy: current.dqaName,
            startedAt: current.startedAt,
            message: `${current.dqaName} is already working on this OE.`,
          });

          return;
        }

        // Same user already owns it.
        if (current) {
          current.expiresAt = now() + LOCK_TIMEOUT;

          send(ws, {
            type: "oe-start-result",
            success: true,
            oeId,
            lockedBy: dqaName,
            startedAt: current.startedAt,
          });

          return;
        }

        // New lock.
        const lock = {
          oeId,
          dqaName,
          startedAt: new Date().toISOString(),
          expiresAt: now() + LOCK_TIMEOUT,
        };

        oeLocks.set(oeId, lock);

        broadcast({
          type: "oe-locked",
          oeId,
          dqaName,
          startedAt: lock.startedAt,
        });

        send(ws, {
          type: "oe-start-result",
          success: true,
          oeId,
          lockedBy: dqaName,
          startedAt: lock.startedAt,
        });

        return;
      }

      // ======================================================
      // DQA HEARTBEAT
      // ======================================================

      if (type === "oe-heartbeat") {
        const oeId = String(message.oeId || "").trim();
        const dqaName = String(message.dqaName || "").trim();

        const lock = oeLocks.get(oeId);

        if (
          lock &&
          lock.dqaName.toLowerCase() === dqaName.toLowerCase()
        ) {
          lock.expiresAt = now() + LOCK_TIMEOUT;
        }

        return;
      }

      // ======================================================
      // DQA UNLOCK
      // ======================================================

      if (type === "oe-unlock") {
        const oeId = String(message.oeId || "").trim();
        const dqaName = String(message.dqaName || "").trim();

        const lock = oeLocks.get(oeId);

        if (
          lock &&
          lock.dqaName.toLowerCase() === dqaName.toLowerCase()
        ) {
          oeLocks.delete(oeId);

          broadcast({
            type: "oe-unlocked",
            oeId,
          });
        }

        return;
      }

      // ======================================================
      // NEW OE SUBMITTED
      // ======================================================

      if (type === "oe-submitted") {
        const payload = {
          type: "oe-submitted",
          oeId: message.oeId || "",
          memberName: message.memberName || "",
          pid: message.pid || "",
          qNumber: message.qNumber || "",
          timestamp:
            message.timestamp || new Date().toISOString(),
        };

        // Tell everyone.
        broadcast(payload);

        return;
      }

      // ======================================================
      // OE STATUS CHANGED
      // APPROVED / REJECTED / RECALLED
      // ======================================================

      if (type === "oe-status-changed") {
        const oeId = String(message.oeId || "").trim();

        const payload = {
          type: "oe-status-changed",
          oeId,
          status: message.status || "",
          memberName: message.memberName || "",
          approvedBy: message.approvedBy || "",
          approvedTime: message.approvedTime || "",
          rejectReason: message.rejectReason || "",
          dqaCorrection: message.dqaCorrection || "",
          timestamp:
            message.timestamp || new Date().toISOString(),
        };

        broadcast(payload);

        return;
      }

      // ======================================================
      // RESULT UPDATE
      // ======================================================

      if (type === "result-updated") {
        broadcast({
          type: "result-updated",
          oeId: message.oeId || "",
          memberName: message.memberName || "",
          status: message.status || "",
          timestamp:
            message.timestamp || new Date().toISOString(),
        });

        return;
      }

      // ======================================================
      // PING
      // ======================================================

      if (type === "ping") {
        send(ws, {
          type: "pong",
          time: new Date().toISOString(),
        });

        return;
      }
    } catch (error) {
      console.error("WebSocket message error:", error);

      send(ws, {
        type: "error",
        message: "Invalid WebSocket message.",
      });
    }
  });

  ws.on("close", () => {
    console.log("Realtime client disconnected");
  });

  ws.on("error", (error) => {
    console.error("WebSocket error:", error);
  });
});

// ============================================================
// START
// ============================================================

server.listen(PORT, () => {
  console.log(
    `Survey Realtime WebSocket server running on port ${PORT}`
  );
});