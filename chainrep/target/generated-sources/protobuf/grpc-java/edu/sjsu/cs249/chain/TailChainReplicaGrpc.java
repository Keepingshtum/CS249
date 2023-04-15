package edu.sjsu.cs249.chain;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.54.0)",
    comments = "Source: chain.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class TailChainReplicaGrpc {

  private TailChainReplicaGrpc() {}

  public static final String SERVICE_NAME = "chain.TailChainReplica";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.GetRequest,
      edu.sjsu.cs249.chain.GetResponse> getGetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "get",
      requestType = edu.sjsu.cs249.chain.GetRequest.class,
      responseType = edu.sjsu.cs249.chain.GetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.GetRequest,
      edu.sjsu.cs249.chain.GetResponse> getGetMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.GetRequest, edu.sjsu.cs249.chain.GetResponse> getGetMethod;
    if ((getGetMethod = TailChainReplicaGrpc.getGetMethod) == null) {
      synchronized (TailChainReplicaGrpc.class) {
        if ((getGetMethod = TailChainReplicaGrpc.getGetMethod) == null) {
          TailChainReplicaGrpc.getGetMethod = getGetMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.chain.GetRequest, edu.sjsu.cs249.chain.GetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "get"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.GetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.GetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TailChainReplicaMethodDescriptorSupplier("get"))
              .build();
        }
      }
    }
    return getGetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TailChainReplicaStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TailChainReplicaStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TailChainReplicaStub>() {
        @java.lang.Override
        public TailChainReplicaStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TailChainReplicaStub(channel, callOptions);
        }
      };
    return TailChainReplicaStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TailChainReplicaBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TailChainReplicaBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TailChainReplicaBlockingStub>() {
        @java.lang.Override
        public TailChainReplicaBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TailChainReplicaBlockingStub(channel, callOptions);
        }
      };
    return TailChainReplicaBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TailChainReplicaFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TailChainReplicaFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TailChainReplicaFutureStub>() {
        @java.lang.Override
        public TailChainReplicaFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TailChainReplicaFutureStub(channel, callOptions);
        }
      };
    return TailChainReplicaFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void get(edu.sjsu.cs249.chain.GetRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.GetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service TailChainReplica.
   */
  public static abstract class TailChainReplicaImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return TailChainReplicaGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service TailChainReplica.
   */
  public static final class TailChainReplicaStub
      extends io.grpc.stub.AbstractAsyncStub<TailChainReplicaStub> {
    private TailChainReplicaStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TailChainReplicaStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TailChainReplicaStub(channel, callOptions);
    }

    /**
     */
    public void get(edu.sjsu.cs249.chain.GetRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.GetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service TailChainReplica.
   */
  public static final class TailChainReplicaBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<TailChainReplicaBlockingStub> {
    private TailChainReplicaBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TailChainReplicaBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TailChainReplicaBlockingStub(channel, callOptions);
    }

    /**
     */
    public edu.sjsu.cs249.chain.GetResponse get(edu.sjsu.cs249.chain.GetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service TailChainReplica.
   */
  public static final class TailChainReplicaFutureStub
      extends io.grpc.stub.AbstractFutureStub<TailChainReplicaFutureStub> {
    private TailChainReplicaFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TailChainReplicaFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TailChainReplicaFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.chain.GetResponse> get(
        edu.sjsu.cs249.chain.GetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET = 0;

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
        case METHODID_GET:
          serviceImpl.get((edu.sjsu.cs249.chain.GetRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.GetResponse>) responseObserver);
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
          getGetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.chain.GetRequest,
              edu.sjsu.cs249.chain.GetResponse>(
                service, METHODID_GET)))
        .build();
  }

  private static abstract class TailChainReplicaBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TailChainReplicaBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return edu.sjsu.cs249.chain.Chain.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TailChainReplica");
    }
  }

  private static final class TailChainReplicaFileDescriptorSupplier
      extends TailChainReplicaBaseDescriptorSupplier {
    TailChainReplicaFileDescriptorSupplier() {}
  }

  private static final class TailChainReplicaMethodDescriptorSupplier
      extends TailChainReplicaBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TailChainReplicaMethodDescriptorSupplier(String methodName) {
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
      synchronized (TailChainReplicaGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TailChainReplicaFileDescriptorSupplier())
              .addMethod(getGetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
