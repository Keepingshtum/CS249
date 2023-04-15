package edu.sjsu.cs249.chain;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.54.0)",
    comments = "Source: chain_debug.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ChainDebugGrpc {

  private ChainDebugGrpc() {}

  public static final String SERVICE_NAME = "chain.ChainDebug";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.ChainDebugRequest,
      edu.sjsu.cs249.chain.ChainDebugResponse> getDebugMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "debug",
      requestType = edu.sjsu.cs249.chain.ChainDebugRequest.class,
      responseType = edu.sjsu.cs249.chain.ChainDebugResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.ChainDebugRequest,
      edu.sjsu.cs249.chain.ChainDebugResponse> getDebugMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.ChainDebugRequest, edu.sjsu.cs249.chain.ChainDebugResponse> getDebugMethod;
    if ((getDebugMethod = ChainDebugGrpc.getDebugMethod) == null) {
      synchronized (ChainDebugGrpc.class) {
        if ((getDebugMethod = ChainDebugGrpc.getDebugMethod) == null) {
          ChainDebugGrpc.getDebugMethod = getDebugMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.chain.ChainDebugRequest, edu.sjsu.cs249.chain.ChainDebugResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "debug"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.ChainDebugRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.ChainDebugResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ChainDebugMethodDescriptorSupplier("debug"))
              .build();
        }
      }
    }
    return getDebugMethod;
  }

  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.ExitRequest,
      edu.sjsu.cs249.chain.ExitResponse> getExitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "exit",
      requestType = edu.sjsu.cs249.chain.ExitRequest.class,
      responseType = edu.sjsu.cs249.chain.ExitResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.ExitRequest,
      edu.sjsu.cs249.chain.ExitResponse> getExitMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.ExitRequest, edu.sjsu.cs249.chain.ExitResponse> getExitMethod;
    if ((getExitMethod = ChainDebugGrpc.getExitMethod) == null) {
      synchronized (ChainDebugGrpc.class) {
        if ((getExitMethod = ChainDebugGrpc.getExitMethod) == null) {
          ChainDebugGrpc.getExitMethod = getExitMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.chain.ExitRequest, edu.sjsu.cs249.chain.ExitResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "exit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.ExitRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.ExitResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ChainDebugMethodDescriptorSupplier("exit"))
              .build();
        }
      }
    }
    return getExitMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ChainDebugStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ChainDebugStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ChainDebugStub>() {
        @java.lang.Override
        public ChainDebugStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ChainDebugStub(channel, callOptions);
        }
      };
    return ChainDebugStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ChainDebugBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ChainDebugBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ChainDebugBlockingStub>() {
        @java.lang.Override
        public ChainDebugBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ChainDebugBlockingStub(channel, callOptions);
        }
      };
    return ChainDebugBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ChainDebugFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ChainDebugFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ChainDebugFutureStub>() {
        @java.lang.Override
        public ChainDebugFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ChainDebugFutureStub(channel, callOptions);
        }
      };
    return ChainDebugFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void debug(edu.sjsu.cs249.chain.ChainDebugRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.ChainDebugResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDebugMethod(), responseObserver);
    }

    /**
     */
    default void exit(edu.sjsu.cs249.chain.ExitRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.ExitResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExitMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ChainDebug.
   */
  public static abstract class ChainDebugImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ChainDebugGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ChainDebug.
   */
  public static final class ChainDebugStub
      extends io.grpc.stub.AbstractAsyncStub<ChainDebugStub> {
    private ChainDebugStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ChainDebugStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ChainDebugStub(channel, callOptions);
    }

    /**
     */
    public void debug(edu.sjsu.cs249.chain.ChainDebugRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.ChainDebugResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDebugMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void exit(edu.sjsu.cs249.chain.ExitRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.ExitResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExitMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ChainDebug.
   */
  public static final class ChainDebugBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ChainDebugBlockingStub> {
    private ChainDebugBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ChainDebugBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ChainDebugBlockingStub(channel, callOptions);
    }

    /**
     */
    public edu.sjsu.cs249.chain.ChainDebugResponse debug(edu.sjsu.cs249.chain.ChainDebugRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDebugMethod(), getCallOptions(), request);
    }

    /**
     */
    public edu.sjsu.cs249.chain.ExitResponse exit(edu.sjsu.cs249.chain.ExitRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExitMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ChainDebug.
   */
  public static final class ChainDebugFutureStub
      extends io.grpc.stub.AbstractFutureStub<ChainDebugFutureStub> {
    private ChainDebugFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ChainDebugFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ChainDebugFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.chain.ChainDebugResponse> debug(
        edu.sjsu.cs249.chain.ChainDebugRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDebugMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.chain.ExitResponse> exit(
        edu.sjsu.cs249.chain.ExitRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExitMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DEBUG = 0;
  private static final int METHODID_EXIT = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DEBUG:
          serviceImpl.debug((edu.sjsu.cs249.chain.ChainDebugRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.ChainDebugResponse>) responseObserver);
          break;
        case METHODID_EXIT:
          serviceImpl.exit((edu.sjsu.cs249.chain.ExitRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.ExitResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getDebugMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.chain.ChainDebugRequest,
              edu.sjsu.cs249.chain.ChainDebugResponse>(
                service, METHODID_DEBUG)))
        .addMethod(
          getExitMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.chain.ExitRequest,
              edu.sjsu.cs249.chain.ExitResponse>(
                service, METHODID_EXIT)))
        .build();
  }

  private static abstract class ChainDebugBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ChainDebugBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return edu.sjsu.cs249.chain.ChainDebugOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ChainDebug");
    }
  }

  private static final class ChainDebugFileDescriptorSupplier
      extends ChainDebugBaseDescriptorSupplier {
    ChainDebugFileDescriptorSupplier() {}
  }

  private static final class ChainDebugMethodDescriptorSupplier
      extends ChainDebugBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ChainDebugMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ChainDebugGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ChainDebugFileDescriptorSupplier())
              .addMethod(getDebugMethod())
              .addMethod(getExitMethod())
              .build();
        }
      }
    }
    return result;
  }
}
