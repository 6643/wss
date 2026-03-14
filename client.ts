// --- Type Definitions ---

type CloseInfo = {
  code?: number;
  reason?: string;
};

// 描述 WebSocketStream 实例的类型
type WSS = {
  readonly readable: ReadableStream<any>;
  readonly writable: WritableStream<any>;
  readonly opened?: Promise<void>;
  readonly closed: Promise<CloseEvent>;
  close(closeInfo?: CloseInfo): void;
};

// 描述 WebSocketStream 构造函数的类型
type WSSFactory = {
  new (url: string, protocols?: string | string[]): WSS;
};

// 连接状态
export enum ConnectionState {
  Connecting, // 0
  Opened, // 1
  Closed, // 2
}

// 消息数据类型
type MessageData = string | ArrayBuffer | Uint8Array;

// 客户端构造选项
export interface WssClientOptions {
  opened?: () => void;
  closed?: () => void;
  erred?: (error: Error) => void;
  stateChanged?: (state: ConnectionState) => void;
  // 最大重连延迟（毫秒）。默认为 0 (不重连)。
  // 当值大于 0 时，开启自动重连功能。
  maxReconnectDelay?: number;
  // 通知重连即将开始。在倒计时期间会每秒调用一次。
  // @param delay 剩余倒计时（毫秒）
  // @param attempts 尝试次数
  reConnecting?: (delay: number, attempts: number) => void;
}

// Wss 客户端公开的接口
export interface WssClient {
  get(): Promise<MessageData>;
  put(data: MessageData): Promise<void>;
  close(): void;
  // 立即触发重连。
  reconnect(): void;
}

// 辅助函数：将 Promise 转换为 [err, data] 元组以消除 try-catch
async function to<T>(
  promise: Promise<T>,
): Promise<[Error | null, T | undefined]> {
  try {
    const data = await promise;
    return [null, data];
  } catch (e) {
    return [e instanceof Error ? e : new Error(String(e)), undefined];
  }
}

// 创建一个新的 WebSocket 客户端实例。
// @param url WebSocket 服务器 URL
// @param options 客户端选项
// @returns 一个实现了 WssClient 接口的对象
export const newWssClient = (
  url: string,
  options: WssClientOptions,
): WssClient => {
  // --- State and Constants ---

  const finalOptions = {
    maxReconnectDelay: 0,
    ...options,
  };

  const nativeWSSFactory = (
    globalThis as typeof globalThis & {
      WebSocketStream?: WSSFactory;
    }
  ).WebSocketStream;
  const RECONNECT_BASE_DELAY = 1000;

  let activeWSS: WSS | undefined;
  let reader: ReadableStreamDefaultReader<MessageData> | undefined;
  let writer: WritableStreamDefaultWriter<MessageData> | undefined;
  let connectionState: ConnectionState = ConnectionState.Closed;
  let putQueue: MessageData[] = [];
  let isManualClose = false;
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  let reconnectingInterval: ReturnType<typeof setInterval> | undefined;
  let reconnectAttempts = 0;
  let nextReconnectTime: number | undefined;
  let forceReconnect = false;

  // --- Internal Helper Functions ---

  function clearReconnectTimers(): void {
    clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
    clearInterval(reconnectingInterval);
    reconnectingInterval = undefined;
    nextReconnectTime = undefined;
  }

  function resetConnectionResources(): void {
    // 资源清理应是原子的，不应受个别错误影响
    if (reader) {
      to(Promise.resolve(reader.releaseLock()));
    }
    if (writer) {
      to(Promise.resolve(writer.releaseLock()));
    }
    reader = undefined;
    writer = undefined;
    activeWSS = undefined;
  }

  async function attachWSS(stream: WSS): Promise<void> {
    activeWSS = stream;
    reader = stream.readable.getReader();
    writer = stream.writable.getWriter();

    // 处理连接建立
    handleOpened(stream);
    // 处理连接关闭
    handleClosed(stream);
  }

  async function handleOpened(stream: WSS): Promise<void> {
    if (!stream.opened) {
      handleConnectionOpen();
      return;
    }

    const [err] = await to(stream.opened);
    if (err) {
      finalOptions.erred?.(err);
      handleConnectionClose();
      return;
    }
    handleConnectionOpen();
  }

  async function handleClosed(stream: WSS): Promise<void> {
    const [err, event] = await to(stream.closed);
    if (err) {
      finalOptions.erred?.(err);
      handleConnectionClose();
      return;
    }
    handleConnectionClose(event);
  }

  function createFallbackWSS(): WSS {
    const webSocket = new WebSocket(url);
    webSocket.binaryType = "arraybuffer";

    let opened = false;
    let openedSettled = false;
    let closedSettled = false;

    let resolveOpened!: () => void;
    let rejectOpened!: (reason?: unknown) => void;
    let resolveClosed!: (event: CloseEvent) => void;
    let rejectClosed!: (reason?: unknown) => void;

    const openedPromise = new Promise<void>((resolve, reject) => {
      resolveOpened = () => {
        if (openedSettled) return;
        openedSettled = true;
        opened = true;
        resolve();
      };
      rejectOpened = (reason?: unknown) => {
        if (openedSettled) return;
        openedSettled = true;
        reject(reason);
      };
    });

    const closed = new Promise<CloseEvent>((resolve, reject) => {
      resolveClosed = (event: CloseEvent) => {
        if (closedSettled) return;
        closedSettled = true;
        resolve(event);
      };
      rejectClosed = (reason?: unknown) => {
        if (closedSettled) return;
        closedSettled = true;
        reject(reason);
      };
    });

    const readable = new ReadableStream<MessageData>({
      start(controller) {
        webSocket.onopen = () => resolveOpened();
        webSocket.onmessage = (event) => controller.enqueue(event.data);
        webSocket.onclose = (event) => {
          if (!opened) rejectOpened(event);
          resolveClosed(event);
          to(Promise.resolve(controller.close()));
        };
        webSocket.onerror = () => {
          const error = new Error("WebSocket fallback error");
          if (!opened) rejectOpened(error);
          rejectClosed(error);
          to(Promise.resolve(controller.error(error)));
        };
      },
      cancel() {
        if (
          webSocket.readyState === WebSocket.CONNECTING ||
          webSocket.readyState === WebSocket.OPEN
        ) {
          webSocket.close(1000, "Readable stream cancelled");
        }
      },
    });

    const writable = new WritableStream<MessageData>({
      async write(data) {
        await openedPromise;
        if (webSocket.readyState !== WebSocket.OPEN) {
          throw new Error("WebSocket is not open.");
        }
        webSocket.send(data);
      },
      async close() {
        if (
          webSocket.readyState === WebSocket.CLOSING ||
          webSocket.readyState === WebSocket.CLOSED
        ) {
          await to(closed);
          return;
        }
        webSocket.close(1000, "Writable stream closed");
        await to(closed);
      },
      async abort(reason) {
        if (
          webSocket.readyState !== WebSocket.CLOSING &&
          webSocket.readyState !== WebSocket.CLOSED
        ) {
          const message =
            reason instanceof Error
              ? reason.message
              : "Writable stream aborted";
          webSocket.close(1000, message);
        }
        await to(closed);
      },
    });

    return {
      readable,
      writable,
      opened: openedPromise,
      closed,
      close(closeInfo?: CloseInfo) {
        if (
          webSocket.readyState === WebSocket.CLOSING ||
          webSocket.readyState === WebSocket.CLOSED
        ) {
          return;
        }
        webSocket.close(
          closeInfo?.code ?? 1000,
          closeInfo?.reason ?? "Client closed",
        );
      },
    };
  }

  // --- Core Logic Functions ---

  function handleConnectionClose(event?: CloseEvent): void {
    if (connectionState === ConnectionState.Closed) return;

    const previousState = connectionState;
    connectionState = ConnectionState.Closed;
    finalOptions.stateChanged?.(connectionState);
    resetConnectionResources();
    clearReconnectTimers();

    if (shouldForceReconnect()) return;

    processReconnect(event, previousState);
  }

  function shouldForceReconnect(): boolean {
    if (!forceReconnect || isManualClose) return false;
    forceReconnect = false;
    reconnectAttempts = 0;
    openConnection();
    return true;
  }

  function processReconnect(
    event: CloseEvent | undefined,
    previousState: ConnectionState,
  ): void {
    forceReconnect = false;
    const isNormalClose = event?.code === 1000;
    const shouldReconnect =
      finalOptions.maxReconnectDelay > 0 && !isManualClose && !isNormalClose;

    if (!shouldReconnect) {
      finalizeClosure(previousState);
      return;
    }
    setupReconnect();
  }

  function finalizeClosure(previousState: ConnectionState): void {
    putQueue = [];
    reconnectAttempts = 0;
    if (previousState === ConnectionState.Opened || isManualClose) {
      finalOptions.closed?.();
    }
  }

  function setupReconnect(): void {
    const delay = Math.min(
      finalOptions.maxReconnectDelay!,
      RECONNECT_BASE_DELAY * 2 ** reconnectAttempts,
    );
    reconnectAttempts++;
    nextReconnectTime = Date.now() + delay;

    finalOptions.reConnecting?.(delay, reconnectAttempts);
    startReconnectTimer(delay);
  }

  function startReconnectTimer(delay: number): void {
    reconnectingInterval = setInterval(() => {
      const remaining = nextReconnectTime ? nextReconnectTime - Date.now() : 0;
      if (remaining <= 0) {
        clearInterval(reconnectingInterval);
        reconnectingInterval = undefined;
        return;
      }
      finalOptions.reConnecting?.(Math.round(remaining), reconnectAttempts);
    }, 1000);

    reconnectTimer = setTimeout(() => {
      clearReconnectTimers();
      openConnection();
    }, delay);
  }

  async function writeMessage(data: MessageData): Promise<void> {
    if (!writer) {
      const err = new Error("Writable stream is not available.");
      finalOptions.erred?.(err);
      throw err;
    }

    const [err] = await to(writer.write(data));
    if (err) {
      finalOptions.erred?.(err);
      throw err;
    }
  }

  function handleConnectionOpen(): void {
    if (connectionState !== ConnectionState.Connecting) return;

    connectionState = ConnectionState.Opened;
    finalOptions.stateChanged?.(connectionState);
    clearReconnectTimers();
    reconnectAttempts = 0;
    finalOptions.opened?.();

    flushPutQueue();
  }

  function flushPutQueue(): void {
    const queue = putQueue;
    putQueue = [];
    queue.forEach((data) => {
      put(data).catch((err) => finalOptions.erred?.(err));
    });
  }

  function openConnection(): void {
    if (
      connectionState === ConnectionState.Connecting ||
      connectionState === ConnectionState.Opened
    ) {
      return;
    }

    isManualClose = false;
    connectionState = ConnectionState.Connecting;
    finalOptions.stateChanged?.(connectionState);

    initWSS();
  }

  async function initWSS(): Promise<void> {
    if (!nativeWSSFactory) {
      attachWSS(createFallbackWSS());
      return;
    }

    const [err, stream] = await to(
      Promise.resolve(new nativeWSSFactory!(url)),
    );
    if (err) {
      finalOptions.erred?.(err);
      attachWSS(createFallbackWSS());
      return;
    }
    attachWSS(stream!);
  }

  // --- Public API ---

  async function get(): Promise<MessageData> {
    if (connectionState === ConnectionState.Closed || !reader) {
      throw new Error("Stream is closed.");
    }

    const [err, result] = await to(reader.read());
    if (err) {
      finalOptions.erred?.(err);
      throw err;
    }
    if (result?.done) {
      throw new Error("Stream has been closed.");
    }
    return result!.value;
  }

  async function put(data: MessageData): Promise<void> {
    if (isManualClose) throw new Error("Stream is manually closed.");

    if (
      connectionState === ConnectionState.Connecting ||
      reconnectTimer ||
      forceReconnect
    ) {
      putQueue.push(data);
      return;
    }
    await writeMessage(data);
  }

  function close(): void {
    if (isManualClose) return;

    isManualClose = true;
    forceReconnect = false;
    clearReconnectTimers();

    activeWSS?.close({ code: 1000, reason: "Client closed" });

    if (connectionState !== ConnectionState.Opened) {
      handleConnectionClose();
    }
  }

  function reconnect(): void {
    clearReconnectTimers();
    reconnectAttempts = 0;
    if (connectionState === ConnectionState.Closed || !activeWSS) {
      openConnection();
      return;
    }

    isManualClose = false;
    forceReconnect = true;
    activeWSS.close({ code: 4000, reason: "Manual reconnect" });
  }

  // --- Initialization ---
  openConnection();

  return {
    get,
    put,
    close,
    reconnect,
  };
};
