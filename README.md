# wss

`wss` 是一个以 stream 为中心的 WebSocket 库，包含：

- Go 服务端
- Go 客户端
- 浏览器端 TypeScript 客户端

它的核心目标不是直接暴露底层 `WebSocket` 读写，而是尽量靠近 `WebSocketStream` 的模型：

- 收到的消息先进入接收 stream，再由 `Get()` 取出
- 要发送的消息先进入发送 stream，再由单独 writer loop 串行写出
- 浏览器端优先使用原生 `WebSocketStream`
- 浏览器不支持 `WebSocketStream` 时，回退到 `WebSocket`，但仍保持 `get/put` 的 stream 使用方式

## 设计特点

- Go 客户端和服务端都采用“接收 stream + 发送 stream”的双向模型
- Go 侧 `Put()` 返回的是“消息实际发送完成后的结果”，不是仅仅表示“入队成功”
- 服务端 `Broadcast()` 基于每个连接自己的发送 stream 扇出
- 服务端 `Close()` 幂等，关闭后会拒绝新连接
- 支持连接空闲超时清理
- 支持自定义 Origin 校验
- 浏览器端支持自动重连，且重连期间 `put()` 会继续排队

## 当前语义

### Go 侧

- `Server.Get(ctx)`:
  从服务端接收 stream 中取出一条消息，返回连接和消息体。
- `Server.Put(ctx, conn, data)`:
  把消息写入该连接的发送 stream，并等待 writer loop 实际写出。
- `Server.Broadcast(ctx, data)`:
  并发地向所有连接的发送 stream 投递消息。
- `Client.Get(ctx)`:
  从客户端接收 stream 中取出一条消息。
- `Client.Put(ctx, data)`:
  把消息写入客户端发送 stream，并等待 writer loop 实际写出。

### 浏览器侧

- `newWssClient(url, options)` 返回的对象暴露 `get()` / `put()` / `close()` / `reconnect()`
- 如果浏览器支持 `WebSocketStream`，直接使用原生实现
- 如果浏览器不支持，则用 `WebSocket` 包一层 `readable` / `writable` / `closed` 兼容层

### 帧类型

- Go 侧接收时同时接受 text frame 和 binary frame，统一以 `[]byte` 暴露
- Go 侧发送时当前统一写出 binary frame

如果你需要保留 text frame / binary frame 的区分，这个库当前还没有暴露消息类型层。

## 安装

本仓库当前 `go.mod` 的模块名就是 `wss`，示例也按这个名称编写：

```go
import "wss"
```

如果你把它发布到自己的仓库，需要替换成实际模块路径。

浏览器端客户端在 [client.ts](/._/lib/go/wss/client.ts)。

## Go 服务端示例

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"stream"
	"wss"
)

func main() {
	srv := wss.NewServer(
		wss.WithServerIOTimeout(30*time.Second),
		wss.WithServerConnIdleTimeout(3*time.Minute),
		wss.WithServerBroadcastConcurrency(16),
		wss.WithServerCheckOrigin(func(r *http.Request) bool {
			return true
		}),
	)

	go func() {
		for {
			conn, data, err := srv.Get(context.Background())
			if err != nil {
				if errors.Is(err, stream.ErrStreamClosed) {
					return
				}
				log.Printf("server get error: %v", err)
				continue
			}

			log.Printf("recv from %s: %s", conn.Addr(), data)

			if err := srv.Put(context.Background(), conn, data); err != nil {
				if errors.Is(err, stream.ErrStreamClosed) {
					return
				}
				log.Printf("server put error: %v", err)
			}
		}
	}()

	http.Handle("/ws", srv)

	log.Println("listen on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
```

## Go 客户端示例

```go
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"stream"
	"wss"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := wss.NewClient(
		ctx,
		"ws://127.0.0.1:8080/ws",
		wss.WithClientIOTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Put(context.Background(), []byte("hello")); err != nil {
		log.Fatal(err)
	}

	msg, err := client.Get(context.Background())
	if err != nil {
		if errors.Is(err, stream.ErrStreamClosed) {
			return
		}
		log.Fatal(err)
	}

	log.Printf("recv: %s", msg)
}
```

## 浏览器端示例

```ts
import { ConnectionState, newWssClient } from "./client";

const client = newWssClient("ws://127.0.0.1:8080/ws", {
    maxReconnectDelay: 30_000,
    opened: () => {
        console.log("opened");
    },
    closed: () => {
        console.log("closed");
    },
    erred: (error) => {
        console.error(error);
    },
    stateChanged: (state) => {
        console.log("state:", ConnectionState[state]);
    },
    reConnecting: (delay, attempts) => {
        console.log(`reconnecting in ${delay}ms, attempt=${attempts}`);
    }
});

await client.put("hello");
const message = await client.get();
console.log("recv:", message);
```

## 超时与关闭行为

- `WithServerIOTimeout(d)` 和 `WithClientIOTimeout(d)` 控制单次读写的内部 deadline
- 当 `d <= 0` 时，库内部不会额外包一层超时 context
- 连接关闭后，再调用 `Put()`，通常会返回 `stream.ErrStreamClosed`
- `Server.Close()` 会关闭所有连接、等待处理协程退出，并关闭服务端接收 stream

## 广播行为

- `Broadcast(ctx, data)` 会先快照当前连接列表，再并发投递
- `WithServerBroadcastConcurrency(n)` 用于限制广播扇出时的并发数
- 当 `n <= 0` 时，内部会自动修正为 `1`
- `Broadcast()` 当前不汇总单连接发送错误；如果需要错误收集，需要额外封装

## 适用场景

- 需要把 WebSocket 收发统一建模成消息流
- 需要在业务层通过阻塞 `Get()` / `Put()` 处理消息
- 需要浏览器端在支持和不支持 `WebSocketStream` 的环境里保持一致的使用方式
- 需要明确的关闭、背压和串行写出语义

## 已知限制

- Go 侧当前不区分 text frame 和 binary frame，接收统一转成 `[]byte`，发送统一使用 binary frame
- `WithServerOpened`、`WithServerClosed`、`WithServerErred` 这几个 option 的回调参数类型是未导出的 `serverConn`
  这意味着它们目前更适合包内使用；如果要给外部包稳定使用，后续更合理的做法是导出连接类型
- README 中没有包含浏览器端完整打包流程，只展示运行时 API

## 测试

运行：

```bash
go test ./...
go test -race ./...
```

当前测试会启动本地临时 HTTP/WebSocket 服务。
