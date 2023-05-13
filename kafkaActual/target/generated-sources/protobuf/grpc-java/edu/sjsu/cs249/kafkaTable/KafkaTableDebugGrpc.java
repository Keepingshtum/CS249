package edu.sjsu.cs249.kafkaTable;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.55.1)",
    comments = "Source: messages.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class KafkaTableDebugGrpc {

  private KafkaTableDebugGrpc() {}

  public static final String SERVICE_NAME = "kafkaTable.KafkaTableDebug";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest,
      edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse> getDebugMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "debug",
      requestType = edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest.class,
      responseType = edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest,
      edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse> getDebugMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest, edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse> getDebugMethod;
    if ((getDebugMethod = KafkaTableDebugGrpc.getDebugMethod) == null) {
      synchronized (KafkaTableDebugGrpc.class) {
        if ((getDebugMethod = KafkaTableDebugGrpc.getDebugMethod) == null) {
          KafkaTableDebugGrpc.getDebugMethod = getDebugMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest, edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "debug"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse.getDefaultInstance()))
              .setSchemaDescriptor(new KafkaTableDebugMethodDescriptorSupplier("debug"))
              .build();
        }
      }
    }
    return getDebugMethod;
  }

  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.ExitRequest,
      edu.sjsu.cs249.kafkaTable.ExitResponse> getExitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "exit",
      requestType = edu.sjsu.cs249.kafkaTable.ExitRequest.class,
      responseType = edu.sjsu.cs249.kafkaTable.ExitResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.ExitRequest,
      edu.sjsu.cs249.kafkaTable.ExitResponse> getExitMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.ExitRequest, edu.sjsu.cs249.kafkaTable.ExitResponse> getExitMethod;
    if ((getExitMethod = KafkaTableDebugGrpc.getExitMethod) == null) {
      synchronized (KafkaTableDebugGrpc.class) {
        if ((getExitMethod = KafkaTableDebugGrpc.getExitMethod) == null) {
          KafkaTableDebugGrpc.getExitMethod = getExitMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.kafkaTable.ExitRequest, edu.sjsu.cs249.kafkaTable.ExitResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "exit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.kafkaTable.ExitRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.kafkaTable.ExitResponse.getDefaultInstance()))
              .setSchemaDescriptor(new KafkaTableDebugMethodDescriptorSupplier("exit"))
              .build();
        }
      }
    }
    return getExitMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static KafkaTableDebugStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<KafkaTableDebugStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<KafkaTableDebugStub>() {
        @java.lang.Override
        public KafkaTableDebugStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new KafkaTableDebugStub(channel, callOptions);
        }
      };
    return KafkaTableDebugStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static KafkaTableDebugBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<KafkaTableDebugBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<KafkaTableDebugBlockingStub>() {
        @java.lang.Override
        public KafkaTableDebugBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new KafkaTableDebugBlockingStub(channel, callOptions);
        }
      };
    return KafkaTableDebugBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static KafkaTableDebugFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<KafkaTableDebugFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<KafkaTableDebugFutureStub>() {
        @java.lang.Override
        public KafkaTableDebugFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new KafkaTableDebugFutureStub(channel, callOptions);
        }
      };
    return KafkaTableDebugFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void debug(edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDebugMethod(), responseObserver);
    }

    /**
     */
    default void exit(edu.sjsu.cs249.kafkaTable.ExitRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.ExitResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExitMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service KafkaTableDebug.
   */
  public static abstract class KafkaTableDebugImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return KafkaTableDebugGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service KafkaTableDebug.
   */
  public static final class KafkaTableDebugStub
      extends io.grpc.stub.AbstractAsyncStub<KafkaTableDebugStub> {
    private KafkaTableDebugStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KafkaTableDebugStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new KafkaTableDebugStub(channel, callOptions);
    }

    /**
     */
    public void debug(edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDebugMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void exit(edu.sjsu.cs249.kafkaTable.ExitRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.ExitResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExitMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service KafkaTableDebug.
   */
  public static final class KafkaTableDebugBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<KafkaTableDebugBlockingStub> {
    private KafkaTableDebugBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KafkaTableDebugBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new KafkaTableDebugBlockingStub(channel, callOptions);
    }

    /**
     */
    public edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse debug(edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDebugMethod(), getCallOptions(), request);
    }

    /**
     */
    public edu.sjsu.cs249.kafkaTable.ExitResponse exit(edu.sjsu.cs249.kafkaTable.ExitRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExitMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service KafkaTableDebug.
   */
  public static final class KafkaTableDebugFutureStub
      extends io.grpc.stub.AbstractFutureStub<KafkaTableDebugFutureStub> {
    private KafkaTableDebugFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KafkaTableDebugFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new KafkaTableDebugFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse> debug(
        edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDebugMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.kafkaTable.ExitResponse> exit(
        edu.sjsu.cs249.kafkaTable.ExitRequest request) {
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
          serviceImpl.debug((edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse>) responseObserver);
          break;
        case METHODID_EXIT:
          serviceImpl.exit((edu.sjsu.cs249.kafkaTable.ExitRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.ExitResponse>) responseObserver);
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
              edu.sjsu.cs249.kafkaTable.KafkaTableDebugRequest,
              edu.sjsu.cs249.kafkaTable.KafkaTableDebugResponse>(
                service, METHODID_DEBUG)))
        .addMethod(
          getExitMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.kafkaTable.ExitRequest,
              edu.sjsu.cs249.kafkaTable.ExitResponse>(
                service, METHODID_EXIT)))
        .build();
  }

  private static abstract class KafkaTableDebugBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    KafkaTableDebugBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return edu.sjsu.cs249.kafkaTable.Messages.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("KafkaTableDebug");
    }
  }

  private static final class KafkaTableDebugFileDescriptorSupplier
      extends KafkaTableDebugBaseDescriptorSupplier {
    KafkaTableDebugFileDescriptorSupplier() {}
  }

  private static final class KafkaTableDebugMethodDescriptorSupplier
      extends KafkaTableDebugBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    KafkaTableDebugMethodDescriptorSupplier(String methodName) {
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
      synchronized (KafkaTableDebugGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new KafkaTableDebugFileDescriptorSupplier())
              .addMethod(getDebugMethod())
              .addMethod(getExitMethod())
              .build();
        }
      }
    }
    return result;
  }
}
