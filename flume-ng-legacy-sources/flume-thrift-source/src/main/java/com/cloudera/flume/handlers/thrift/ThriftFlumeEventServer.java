/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.cloudera.flume.handlers.thrift;

import org.apache.commons.lang.builder.HashCodeBuilder;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class ThriftFlumeEventServer {

  public interface Iface {

    public void append(ThriftFlumeEvent evt) throws org.apache.thrift.TException;

    public void close() throws org.apache.thrift.TException;

  }

  public interface AsyncIface {

    public void append(ThriftFlumeEvent evt, org.apache.thrift.async.AsyncMethodCallback<AsyncClient.append_call> resultHandler) throws org.apache.thrift.TException;

    public void close(org.apache.thrift.async.AsyncMethodCallback<AsyncClient.close_call> resultHandler) throws org.apache.thrift.TException;

  }

  public static class Client implements org.apache.thrift.TServiceClient, Iface {
    public static class Factory implements org.apache.thrift.TServiceClientFactory<Client> {
      public Factory() {}
      public Client getClient(org.apache.thrift.protocol.TProtocol prot) {
        return new Client(prot);
      }
      public Client getClient(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
        return new Client(iprot, oprot);
      }
    }

    public Client(org.apache.thrift.protocol.TProtocol prot)
    {
      this(prot, prot);
    }

    public Client(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot)
    {
      iprot_ = iprot;
      oprot_ = oprot;
    }

    protected org.apache.thrift.protocol.TProtocol iprot_;
    protected org.apache.thrift.protocol.TProtocol oprot_;

    protected int seqid_;

    public org.apache.thrift.protocol.TProtocol getInputProtocol()
    {
      return this.iprot_;
    }

    public org.apache.thrift.protocol.TProtocol getOutputProtocol()
    {
      return this.oprot_;
    }

    public void append(ThriftFlumeEvent evt) throws org.apache.thrift.TException
    {
      send_append(evt);
    }

    public void send_append(ThriftFlumeEvent evt) throws org.apache.thrift.TException
    {
      oprot_.writeMessageBegin(new org.apache.thrift.protocol.TMessage("append", org.apache.thrift.protocol.TMessageType.CALL, ++seqid_));
      append_args args = new append_args();
      args.setEvt(evt);
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void close() throws org.apache.thrift.TException
    {
      send_close();
      recv_close();
    }

    public void send_close() throws org.apache.thrift.TException
    {
      oprot_.writeMessageBegin(new org.apache.thrift.protocol.TMessage("close", org.apache.thrift.protocol.TMessageType.CALL, ++seqid_));
      close_args args = new close_args();
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_close() throws org.apache.thrift.TException
    {
      org.apache.thrift.protocol.TMessage msg = iprot_.readMessageBegin();
      if (msg.type == org.apache.thrift.protocol.TMessageType.EXCEPTION) {
        org.apache.thrift.TApplicationException x = org.apache.thrift.TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      if (msg.seqid != seqid_) {
        throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.BAD_SEQUENCE_ID, "close failed: out of sequence response");
      }
      close_result result = new close_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      return;
    }

  }
  public static class AsyncClient extends org.apache.thrift.async.TAsyncClient implements AsyncIface {
    public static class Factory implements org.apache.thrift.async.TAsyncClientFactory<AsyncClient> {
      private org.apache.thrift.async.TAsyncClientManager clientManager;
      private org.apache.thrift.protocol.TProtocolFactory protocolFactory;
      public Factory(org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
        this.clientManager = clientManager;
        this.protocolFactory = protocolFactory;
      }
      public AsyncClient getAsyncClient(org.apache.thrift.transport.TNonblockingTransport transport) {
        return new AsyncClient(protocolFactory, clientManager, transport);
      }
    }

    public AsyncClient(org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.transport.TNonblockingTransport transport) {
      super(protocolFactory, clientManager, transport);
    }

    public void append(ThriftFlumeEvent evt, org.apache.thrift.async.AsyncMethodCallback<append_call> resultHandler) throws org.apache.thrift.TException {
      checkReady();
      append_call method_call = new append_call(evt, resultHandler, this, protocolFactory, transport);
      this.currentMethod = method_call;
      manager.call(method_call);
    }

    public static class append_call extends org.apache.thrift.async.TAsyncMethodCall {
      private ThriftFlumeEvent evt;
      public append_call(ThriftFlumeEvent evt, org.apache.thrift.async.AsyncMethodCallback<append_call> resultHandler, org.apache.thrift.async.TAsyncClient client, org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.transport.TNonblockingTransport transport) throws org.apache.thrift.TException {
        super(client, protocolFactory, transport, resultHandler, true);
        this.evt = evt;
      }

      public void write_args(org.apache.thrift.protocol.TProtocol prot) throws org.apache.thrift.TException {
        prot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("append", org.apache.thrift.protocol.TMessageType.CALL, 0));
        append_args args = new append_args();
        args.setEvt(evt);
        args.write(prot);
        prot.writeMessageEnd();
      }

      public void getResult() throws org.apache.thrift.TException {
        if (getState() != org.apache.thrift.async.TAsyncMethodCall.State.RESPONSE_READ) {
          throw new IllegalStateException("Method call not finished!");
        }
        org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(getFrameBuffer().array());
        org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
      }
    }

    public void close(org.apache.thrift.async.AsyncMethodCallback<close_call> resultHandler) throws org.apache.thrift.TException {
      checkReady();
      close_call method_call = new close_call(resultHandler, this, protocolFactory, transport);
      this.currentMethod = method_call;
      manager.call(method_call);
    }

    public static class close_call extends org.apache.thrift.async.TAsyncMethodCall {
      public close_call(org.apache.thrift.async.AsyncMethodCallback<close_call> resultHandler, org.apache.thrift.async.TAsyncClient client, org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.transport.TNonblockingTransport transport) throws org.apache.thrift.TException {
        super(client, protocolFactory, transport, resultHandler, false);
      }

      public void write_args(org.apache.thrift.protocol.TProtocol prot) throws org.apache.thrift.TException {
        prot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("close", org.apache.thrift.protocol.TMessageType.CALL, 0));
        close_args args = new close_args();
        args.write(prot);
        prot.writeMessageEnd();
      }

      public void getResult() throws org.apache.thrift.TException {
        if (getState() != org.apache.thrift.async.TAsyncMethodCall.State.RESPONSE_READ) {
          throw new IllegalStateException("Method call not finished!");
        }
        org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(getFrameBuffer().array());
        org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
        (new Client(prot)).recv_close();
      }
    }

  }

  public static class Processor implements org.apache.thrift.TProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class.getName());
    public Processor(Iface iface)
    {
      iface_ = iface;
      processMap_.put("append", new append());
      processMap_.put("close", new close());
    }

    protected static interface ProcessFunction {
      public void process(int seqid, org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException;
    }

    private Iface iface_;
    protected final HashMap<String,ProcessFunction> processMap_ = new HashMap<String,ProcessFunction>();

    public boolean process(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException
    {
      org.apache.thrift.protocol.TMessage msg = iprot.readMessageBegin();
      ProcessFunction fn = processMap_.get(msg.name);
      if (fn == null) {
        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, org.apache.thrift.protocol.TType.STRUCT);
        iprot.readMessageEnd();
        org.apache.thrift.TApplicationException x = new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.UNKNOWN_METHOD, "Invalid method name: '"+msg.name+"'");
        oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage(msg.name, org.apache.thrift.protocol.TMessageType.EXCEPTION, msg.seqid));
        x.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
        return true;
      }
      fn.process(msg.seqid, iprot, oprot);
      return true;
    }

    private class append implements ProcessFunction {
      public void process(int seqid, org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException
      {
        append_args args = new append_args();
        try {
          args.read(iprot);
        } catch (org.apache.thrift.protocol.TProtocolException e) {
          iprot.readMessageEnd();
          org.apache.thrift.TApplicationException x = new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.PROTOCOL_ERROR, e.getMessage());
          oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("append", org.apache.thrift.protocol.TMessageType.EXCEPTION, seqid));
          x.write(oprot);
          oprot.writeMessageEnd();
          oprot.getTransport().flush();
          return;
        }
        iprot.readMessageEnd();
        iface_.append(args.evt);
        return;
      }
    }

    private class close implements ProcessFunction {
      public void process(int seqid, org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException
      {
        close_args args = new close_args();
        try {
          args.read(iprot);
        } catch (org.apache.thrift.protocol.TProtocolException e) {
          iprot.readMessageEnd();
          org.apache.thrift.TApplicationException x = new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.PROTOCOL_ERROR, e.getMessage());
          oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("close", org.apache.thrift.protocol.TMessageType.EXCEPTION, seqid));
          x.write(oprot);
          oprot.writeMessageEnd();
          oprot.getTransport().flush();
          return;
        }
        iprot.readMessageEnd();
        close_result result = new close_result();
        iface_.close();
        oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("close", org.apache.thrift.protocol.TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

  }

  public static class append_args implements org.apache.thrift.TBase<append_args, append_args._Fields>, java.io.Serializable, Cloneable   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("append_args");

    private static final org.apache.thrift.protocol.TField EVT_FIELD_DESC = new org.apache.thrift.protocol.TField("evt", org.apache.thrift.protocol.TType.STRUCT, (short)1);

    public ThriftFlumeEvent evt;

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      EVT((short)1, "evt");

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 1: // EVT
            return EVT;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments

    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.EVT, new org.apache.thrift.meta_data.FieldMetaData("evt", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ThriftFlumeEvent.class)));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(append_args.class, metaDataMap);
    }

    public append_args() {
    }

    public append_args(
      ThriftFlumeEvent evt)
    {
      this();
      this.evt = evt;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public append_args(append_args other) {
      if (other.isSetEvt()) {
        this.evt = new ThriftFlumeEvent(other.evt);
      }
    }

    public append_args deepCopy() {
      return new append_args(this);
    }

    @Override
    public void clear() {
      this.evt = null;
    }

    public ThriftFlumeEvent getEvt() {
      return this.evt;
    }

    public append_args setEvt(ThriftFlumeEvent evt) {
      this.evt = evt;
      return this;
    }

    public void unsetEvt() {
      this.evt = null;
    }

    /** Returns true if field evt is set (has been assigned a value) and false otherwise */
    public boolean isSetEvt() {
      return this.evt != null;
    }

    public void setEvtIsSet(boolean value) {
      if (!value) {
        this.evt = null;
      }
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case EVT:
        if (value == null) {
          unsetEvt();
        } else {
          setEvt((ThriftFlumeEvent)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case EVT:
        return getEvt();

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case EVT:
        return isSetEvt();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof append_args)
        return this.equals((append_args)that);
      return false;
    }

    public boolean equals(append_args that) {
      if (that == null)
        return false;

      boolean this_present_evt = true && this.isSetEvt();
      boolean that_present_evt = true && that.isSetEvt();
      if (this_present_evt || that_present_evt) {
        if (!(this_present_evt && that_present_evt))
          return false;
        if (!this.evt.equals(that.evt))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder builder = new HashCodeBuilder();

      boolean present_evt = true && (isSetEvt());
      builder.append(present_evt);
      if (present_evt)
        builder.append(evt);

      return builder.toHashCode();
    }

    public int compareTo(append_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;
      append_args typedOther = (append_args)other;

      lastComparison = Boolean.valueOf(isSetEvt()).compareTo(typedOther.isSetEvt());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetEvt()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.evt, typedOther.evt);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (field.id) {
          case 1: // EVT
            if (field.type == org.apache.thrift.protocol.TType.STRUCT) {
              this.evt = new ThriftFlumeEvent();
              this.evt.read(iprot);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.evt != null) {
        oprot.writeFieldBegin(EVT_FIELD_DESC);
        this.evt.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("append_args(");
      boolean first = true;

      sb.append("evt:");
      if (this.evt == null) {
        sb.append("null");
      } else {
        sb.append(this.evt);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

  }

  public static class close_args implements org.apache.thrift.TBase<close_args, close_args._Fields>, java.io.Serializable, Cloneable   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("close_args");



    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
;

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(close_args.class, metaDataMap);
    }

    public close_args() {
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public close_args(close_args other) {
    }

    public close_args deepCopy() {
      return new close_args(this);
    }

    @Override
    public void clear() {
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof close_args)
        return this.equals((close_args)that);
      return false;
    }

    public boolean equals(close_args that) {
      if (that == null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder builder = new HashCodeBuilder();

      return builder.toHashCode();
    }

    public int compareTo(close_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;
      close_args typedOther = (close_args)other;

      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (field.id) {
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("close_args(");
      boolean first = true;

      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

  }

  public static class close_result implements org.apache.thrift.TBase<close_result, close_result._Fields>, java.io.Serializable, Cloneable   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("close_result");



    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
;

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(close_result.class, metaDataMap);
    }

    public close_result() {
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public close_result(close_result other) {
    }

    public close_result deepCopy() {
      return new close_result(this);
    }

    @Override
    public void clear() {
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof close_result)
        return this.equals((close_result)that);
      return false;
    }

    public boolean equals(close_result that) {
      if (that == null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder builder = new HashCodeBuilder();

      return builder.toHashCode();
    }

    public int compareTo(close_result other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;
      close_result typedOther = (close_result)other;

      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (field.id) {
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      oprot.writeStructBegin(STRUCT_DESC);

      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("close_result(");
      boolean first = true;

      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

  }

}