package edu.sjsu.cs249.chain;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.54.0)",
    comments = "Source: chain.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ReplicaGrpc {

  private ReplicaGrpc() {}

  public static final String SERVICE_NAME = "chain.Replica";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.UpdateRequest,
      edu.sjsu.cs249.chain.UpdateResponse> getUpdateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "update",
      requestType = edu.sjsu.cs249.chain.UpdateRequest.class,
      responseType = edu.sjsu.cs249.chain.UpdateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.UpdateRequest,
      edu.sjsu.cs249.chain.UpdateResponse> getUpdateMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.UpdateRequest, edu.sjsu.cs249.chain.UpdateResponse> getUpdateMethod;
    if ((getUpdateMethod = ReplicaGrpc.getUpdateMethod) == null) {
      synchronized (ReplicaGrpc.class) {
        if ((getUpdateMethod = ReplicaGrpc.getUpdateMethod) == null) {
          ReplicaGrpc.getUpdateMethod = getUpdateMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.chain.UpdateRequest, edu.sjsu.cs249.chain.UpdateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "update"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.UpdateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.UpdateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ReplicaMethodDescriptorSupplier("update"))
              .build();
        }
      }
    }
    return getUpdateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.NewSuccessorRequest,
      edu.sjsu.cs249.chain.NewSuccessorResponse> getNewSuccessorMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "newSuccessor",
      requestType = edu.sjsu.cs249.chain.NewSuccessorRequest.class,
      responseType = edu.sjsu.cs249.chain.NewSuccessorResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.NewSuccessorRequest,
      edu.sjsu.cs249.chain.NewSuccessorResponse> getNewSuccessorMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.NewSuccessorRequest, edu.sjsu.cs249.chain.NewSuccessorResponse> getNewSuccessorMethod;
    if ((getNewSuccessorMethod = ReplicaGrpc.getNewSuccessorMethod) == null) {
      synchronized (ReplicaGrpc.class) {
        if ((getNewSuccessorMethod = ReplicaGrpc.getNewSuccessorMethod) == null) {
          ReplicaGrpc.getNewSuccessorMethod = getNewSuccessorMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.chain.NewSuccessorRequest, edu.sjsu.cs249.chain.NewSuccessorResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "newSuccessor"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.NewSuccessorRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.NewSuccessorResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ReplicaMethodDescriptorSupplier("newSuccessor"))
              .build();
        }
      }
    }
    return getNewSuccessorMethod;
  }

  private static volatile io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.AckRequest,
      edu.sjsu.cs249.chain.AckResponse> getAckMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ack",
      requestType = edu.sjsu.cs249.chain.AckRequest.class,
      responseType = edu.sjsu.cs249.chain.AckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.AckRequest,
      edu.sjsu.cs249.chain.AckResponse> getAckMethod() {
    io.grpc.MethodDescriptor<edu.sjsu.cs249.chain.AckRequest, edu.sjsu.cs249.chain.AckResponse> getAckMethod;
    if ((getAckMethod = ReplicaGrpc.getAckMethod) == null) {
      synchronized (ReplicaGrpc.class) {
        if ((getAckMethod = ReplicaGrpc.getAckMethod) == null) {
          ReplicaGrpc.getAckMethod = getAckMethod =
              io.grpc.MethodDescriptor.<edu.sjsu.cs249.chain.AckRequest, edu.sjsu.cs249.chain.AckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ack"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.AckRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  edu.sjsu.cs249.chain.AckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ReplicaMethodDescriptorSupplier("ack"))
              .build();
        }
      }
    }
    return getAckMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ReplicaStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ReplicaStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ReplicaStub>() {
        @java.lang.Override
        public ReplicaStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ReplicaStub(channel, callOptions);
        }
      };
    return ReplicaStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ReplicaBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ReplicaBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ReplicaBlockingStub>() {
        @java.lang.Override
        public ReplicaBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ReplicaBlockingStub(channel, callOptions);
        }
      };
    return ReplicaBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ReplicaFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ReplicaFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ReplicaFutureStub>() {
        @java.lang.Override
        public ReplicaFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ReplicaFutureStub(channel, callOptions);
        }
      };
    return ReplicaFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void update(edu.sjsu.cs249.chain.UpdateRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.UpdateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateMethod(), responseObserver);
    }

    /**
     * <pre>
     * will be called by a new successor to this replica
     * </pre>
     */
    default void newSuccessor(edu.sjsu.cs249.chain.NewSuccessorRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.NewSuccessorResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNewSuccessorMethod(), responseObserver);
    }

    /**
     */
    default void ack(edu.sjsu.cs249.chain.AckRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.AckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAckMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Replica.
   */
  public static abstract class ReplicaImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ReplicaGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Replica.
   */
  public static final class ReplicaStub
      extends io.grpc.stub.AbstractAsyncStub<ReplicaStub> {
    private ReplicaStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ReplicaStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ReplicaStub(channel, callOptions);
    }

    /**
     */
    public void update(edu.sjsu.cs249.chain.UpdateRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.UpdateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * will be called by a new successor to this replica
     * </pre>
     */
    public void newSuccessor(edu.sjsu.cs249.chain.NewSuccessorRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.NewSuccessorResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNewSuccessorMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void ack(edu.sjsu.cs249.chain.AckRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.AckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAckMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Replica.
   */
  public static final class ReplicaBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ReplicaBlockingStub> {
    private ReplicaBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ReplicaBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ReplicaBlockingStub(channel, callOptions);
    }

    /**
     */
    public edu.sjsu.cs249.chain.UpdateResponse update(edu.sjsu.cs249.chain.UpdateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * will be called by a new successor to this replica
     * </pre>
     */
    public edu.sjsu.cs249.chain.NewSuccessorResponse newSuccessor(edu.sjsu.cs249.chain.NewSuccessorRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNewSuccessorMethod(), getCallOptions(), request);
    }

    /**
     */
    public edu.sjsu.cs249.chain.AckResponse ack(edu.sjsu.cs249.chain.AckRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAckMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Replica.
   */
  public static final class ReplicaFutureStub
      extends io.grpc.stub.AbstractFutureStub<ReplicaFutureStub> {
    private ReplicaFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ReplicaFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ReplicaFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.chain.UpdateResponse> update(
        edu.sjsu.cs249.chain.UpdateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * will be called by a new successor to this replica
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.chain.NewSuccessorResponse> newSuccessor(
        edu.sjsu.cs249.chain.NewSuccessorRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNewSuccessorMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.sjsu.cs249.chain.AckResponse> ack(
        edu.sjsu.cs249.chain.AckRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAckMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_UPDATE = 0;
  private static final int METHODID_NEW_SUCCESSOR = 1;
  private static final int METHODID_ACK = 2;

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
        case METHODID_UPDATE:
          serviceImpl.update((edu.sjsu.cs249.chain.UpdateRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.UpdateResponse>) responseObserver);
          break;
        case METHODID_NEW_SUCCESSOR:
          serviceImpl.newSuccessor((edu.sjsu.cs249.chain.NewSuccessorRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.NewSuccessorResponse>) responseObserver);
          break;
        case METHODID_ACK:
          serviceImpl.ack((edu.sjsu.cs249.chain.AckRequest) request,
              (io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.AckResponse>) responseObserver);
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
          getUpdateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.chain.UpdateRequest,
              edu.sjsu.cs249.chain.UpdateResponse>(
                service, METHODID_UPDATE)))
        .addMethod(
          getNewSuccessorMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.chain.NewSuccessorRequest,
              edu.sjsu.cs249.chain.NewSuccessorResponse>(
                service, METHODID_NEW_SUCCESSOR)))
        .addMethod(
          getAckMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              edu.sjsu.cs249.chain.AckRequest,
              edu.sjsu.cs249.chain.AckResponse>(
                service, METHODID_ACK)))
        .build();
  }

  private static abstract class ReplicaBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ReplicaBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return edu.sjsu.cs249.chain.Chain.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Replica");
    }
  }

  private static final class ReplicaFileDescriptorSupplier
      extends ReplicaBaseDescriptorSupplier {
    ReplicaFileDescriptorSupplier() {}
  }

  private static final class ReplicaMethodDescriptorSupplier
      extends ReplicaBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ReplicaMethodDescriptorSupplier(String methodName) {
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
      synchronized (ReplicaGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ReplicaFileDescriptorSupplier())
              .addMethod(getUpdateMethod())
              .addMethod(getNewSuccessorMethod())
              .addMethod(getAckMethod())
              .build();
        }
      }
    }
    return result;
  }
}
