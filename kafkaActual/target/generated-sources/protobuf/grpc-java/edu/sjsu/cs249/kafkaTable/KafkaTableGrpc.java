package edu.sjsu.cs249.kafkaTable;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.55.1)",
    comments = "Source: messages.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class KafkaTableGrpc {

  private KafkaTableGrpc() {}

  public static final String SERVICE_NAME = "kafkaTable.KafkaTable";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.IncRequest,
      edu.sjsu.cs249.kafkaTable.IncResponse> getIncMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "inc",
      requestType = edu.sjsu.cs249.kafkaTable.IncRequest.class,
      responseType = edu.sjsu.cs249.kafkaTable.IncResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.IncRequest,
      edu.sjsu.cs249.kafkaTable.IncResponse> getIncMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.IncRequest, edu.sjsu.cs249.kafkaTable.IncResponse> getIncMethod;
    if ((getIncMethod = KafkaTableGrpc.getIncMethod) == null) {
      synchronized (KafkaTableGrpc.class) {
        if ((getIncMethod = KafkaTableGrpc.getIncMethod) == null) {
          KafkaTableGrpc.getIncMethod = getIncMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.kafkaTable.IncRequest, edu.sjsu.cs249.kafkaTable.IncResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "inc"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.kafkaTable.IncRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.kafkaTable.IncResponse.getDefaultInstance()))
              .setSchemaDescriptor(new KafkaTableMethodDescriptorSupplier("inc"))
              .build();
        }
      }
    }
    return getIncMethod;
  }

  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.GetRequest,
      edu.sjsu.cs249.kafkaTable.GetResponse> getGetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "get",
      requestType = edu.sjsu.cs249.kafkaTable.GetRequest.class,
      responseType = edu.sjsu.cs249.kafkaTable.GetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.GetRequest,
      edu.sjsu.cs249.kafkaTable.GetResponse> getGetMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.kafkaTable.GetRequest, edu.sjsu.cs249.kafkaTable.GetResponse> getGetMethod;
    if ((getGetMethod = KafkaTableGrpc.getGetMethod) == null) {
      synchronized (KafkaTableGrpc.class) {
        if ((getGetMethod = KafkaTableGrpc.getGetMethod) == null) {
          KafkaTableGrpc.getGetMethod = getGetMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.kafkaTable.GetRequest, edu.sjsu.cs249.kafkaTable.GetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "get"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.kafkaTable.GetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.kafkaTable.GetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new KafkaTableMethodDescriptorSupplier("get"))
              .build();
        }
      }
    }
    return getGetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static KafkaTableStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<KafkaTableStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<KafkaTableStub>() {
        @java.lang.Override
        public KafkaTableStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new KafkaTableStub(channel, callOptions);
        }
      };
    return KafkaTableStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static KafkaTableBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<KafkaTableBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<KafkaTableBlockingStub>() {
        @java.lang.Override
        public KafkaTableBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new KafkaTableBlockingStub(channel, callOptions);
        }
      };
    return KafkaTableBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static KafkaTableFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<KafkaTableFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<KafkaTableFutureStub>() {
        @java.lang.Override
        public KafkaTableFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new KafkaTableFutureStub(channel, callOptions);
        }
      };
    return KafkaTableFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void inc(edu.sjsu.cs249.kafkaTable.IncRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.IncResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getIncMethod(), responseObserver);
    }

    /**
     */
    default void get(edu.sjsu.cs249.kafkaTable.GetRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.GetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service KafkaTable.
   */
  public static abstract class KafkaTableImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return KafkaTableGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service KafkaTable.
   */
  public static final class KafkaTableStub
      extends io.grpc.stub.AbstractAsyncStub<KafkaTableStub> {
    private KafkaTableStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KafkaTableStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new KafkaTableStub(channel, callOptions);
    }

    /**
     */
    public void inc(edu.sjsu.cs249.kafkaTable.IncRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.IncResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getIncMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void get(edu.sjsu.cs249.kafkaTable.GetRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.GetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service KafkaTable.
   */
  public static final class KafkaTableBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<KafkaTableBlockingStub> {
    private KafkaTableBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KafkaTableBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new KafkaTableBlockingStub(channel, callOptions);
    }

    /**
     */
    public edu.sjsu.cs249.kafkaTable.IncResponse inc(edu.sjsu.cs249.kafkaTable.IncRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getIncMethod(), getCallOptions(), request);
    }

    /**
     */
    public edu.sjsu.cs249.kafkaTable.GetResponse get(edu.sjsu.cs249.kafkaTable.GetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service KafkaTable.
   */
  public static final class KafkaTableFutureStub
      extends io.grpc.stub.AbstractFutureStub<KafkaTableFutureStub> {
    private KafkaTableFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KafkaTableFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new KafkaTableFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.kafkaTable.IncResponse> inc(
        edu.sjsu.cs249.kafkaTable.IncRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getIncMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.kafkaTable.GetResponse> get(
        edu.sjsu.cs249.kafkaTable.GetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_INC = 0;
  private static final int METHODID_GET = 1;

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
        case METHODID_INC:
          serviceImpl.inc((edu.sjsu.cs249.kafkaTable.IncRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.IncResponse>) responseObserver);
          break;
        case METHODID_GET:
          serviceImpl.get((edu.sjsu.cs249.kafkaTable.GetRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.kafkaTable.GetResponse>) responseObserver);
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
          getIncMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.kafkaTable.IncRequest,
              edu.sjsu.cs249.kafkaTable.IncResponse>(
                service, METHODID_INC)))
        .addMethod(
          getGetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.kafkaTable.GetRequest,
              edu.sjsu.cs249.kafkaTable.GetResponse>(
                service, METHODID_GET)))
        .build();
  }

  private static abstract class KafkaTableBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    KafkaTableBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return edu.sjsu.cs249.kafkaTable.Messages.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("KafkaTable");
    }
  }

  private static final class KafkaTableFileDescriptorSupplier
      extends KafkaTableBaseDescriptorSupplier {
    KafkaTableFileDescriptorSupplier() {}
  }

  private static final class KafkaTableMethodDescriptorSupplier
      extends KafkaTableBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    KafkaTableMethodDescriptorSupplier(String methodName) {
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
      synchronized (KafkaTableGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new KafkaTableFileDescriptorSupplier())
              .addMethod(getIncMethod())
              .addMethod(getGetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
