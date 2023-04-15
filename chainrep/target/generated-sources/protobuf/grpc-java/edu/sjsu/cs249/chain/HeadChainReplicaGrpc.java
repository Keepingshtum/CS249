package edu.sjsu.cs249.chain;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.54.0)",
    comments = "Source: chain.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class HeadChainReplicaGrpc {

  private HeadChainReplicaGrpc() {}

  public static final String SERVICE_NAME = "chain.HeadChainReplica";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.IncRequest,
      edu.sjsu.cs249.chain.HeadResponse> getIncrementMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "increment",
      requestType = edu.sjsu.cs249.chain.IncRequest.class,
      responseType = edu.sjsu.cs249.chain.HeadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.IncRequest,
      edu.sjsu.cs249.chain.HeadResponse> getIncrementMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.IncRequest, edu.sjsu.cs249.chain.HeadResponse> getIncrementMethod;
    if ((getIncrementMethod = HeadChainReplicaGrpc.getIncrementMethod) == null) {
      synchronized (HeadChainReplicaGrpc.class) {
        if ((getIncrementMethod = HeadChainReplicaGrpc.getIncrementMethod) == null) {
          HeadChainReplicaGrpc.getIncrementMethod = getIncrementMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.chain.IncRequest, edu.sjsu.cs249.chain.HeadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "increment"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.IncRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.HeadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HeadChainReplicaMethodDescriptorSupplier("increment"))
              .build();
        }
      }
    }
    return getIncrementMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static HeadChainReplicaStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HeadChainReplicaStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HeadChainReplicaStub>() {
        @java.lang.Override
        public HeadChainReplicaStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HeadChainReplicaStub(channel, callOptions);
        }
      };
    return HeadChainReplicaStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static HeadChainReplicaBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HeadChainReplicaBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HeadChainReplicaBlockingStub>() {
        @java.lang.Override
        public HeadChainReplicaBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HeadChainReplicaBlockingStub(channel, callOptions);
        }
      };
    return HeadChainReplicaBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static HeadChainReplicaFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HeadChainReplicaFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HeadChainReplicaFutureStub>() {
        @java.lang.Override
        public HeadChainReplicaFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HeadChainReplicaFutureStub(channel, callOptions);
        }
      };
    return HeadChainReplicaFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void increment(edu.sjsu.cs249.chain.IncRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.HeadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getIncrementMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service HeadChainReplica.
   */
  public static abstract class HeadChainReplicaImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return HeadChainReplicaGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service HeadChainReplica.
   */
  public static final class HeadChainReplicaStub
      extends io.grpc.stub.AbstractAsyncStub<HeadChainReplicaStub> {
    private HeadChainReplicaStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HeadChainReplicaStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HeadChainReplicaStub(channel, callOptions);
    }

    /**
     */
    public void increment(edu.sjsu.cs249.chain.IncRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.HeadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getIncrementMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service HeadChainReplica.
   */
  public static final class HeadChainReplicaBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<HeadChainReplicaBlockingStub> {
    private HeadChainReplicaBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HeadChainReplicaBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HeadChainReplicaBlockingStub(channel, callOptions);
    }

    /**
     */
    public edu.sjsu.cs249.chain.HeadResponse increment(edu.sjsu.cs249.chain.IncRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getIncrementMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service HeadChainReplica.
   */
  public static final class HeadChainReplicaFutureStub
      extends io.grpc.stub.AbstractFutureStub<HeadChainReplicaFutureStub> {
    private HeadChainReplicaFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HeadChainReplicaFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HeadChainReplicaFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.chain.HeadResponse> increment(
        edu.sjsu.cs249.chain.IncRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getIncrementMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_INCREMENT = 0;

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
        case METHODID_INCREMENT:
          serviceImpl.increment((edu.sjsu.cs249.chain.IncRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.HeadResponse>) responseObserver);
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
          getIncrementMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.chain.IncRequest,
              edu.sjsu.cs249.chain.HeadResponse>(
                service, METHODID_INCREMENT)))
        .build();
  }

  private static abstract class HeadChainReplicaBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    HeadChainReplicaBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return edu.sjsu.cs249.chain.Chain.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("HeadChainReplica");
    }
  }

  private static final class HeadChainReplicaFileDescriptorSupplier
      extends HeadChainReplicaBaseDescriptorSupplier {
    HeadChainReplicaFileDescriptorSupplier() {}
  }

  private static final class HeadChainReplicaMethodDescriptorSupplier
      extends HeadChainReplicaBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    HeadChainReplicaMethodDescriptorSupplier(String methodName) {
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
      synchronized (HeadChainReplicaGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new HeadChainReplicaFileDescriptorSupplier())
              .addMethod(getIncrementMethod())
              .build();
        }
      }
    }
    return result;
  }
}
