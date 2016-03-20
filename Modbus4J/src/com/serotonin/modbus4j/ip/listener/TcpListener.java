/*
 * ============================================================================
 * GNU General Public License
 * ============================================================================
 *
 * Copyright (C) 2014 - MCA Desenvolvimento de Sistemas Ltda - http://www.mcasistemas.com.br
 * @author Diego R. Ferreira
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.serotonin.modbus4j.ip.listener;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.serotonin.modbus4j.ModbusMaster;
import com.serotonin.modbus4j.base.BaseMessageParser;
import com.serotonin.modbus4j.exception.ModbusInitException;
import com.serotonin.modbus4j.exception.ModbusTransportException;
import com.serotonin.modbus4j.ip.IpMessageResponse;
import com.serotonin.modbus4j.ip.IpParameters;
import com.serotonin.modbus4j.ip.encap.EncapMessageParser;
import com.serotonin.modbus4j.ip.encap.EncapMessageRequest;
import com.serotonin.modbus4j.ip.encap.EncapWaitingRoomKeyFactory;
import com.serotonin.modbus4j.ip.xa.XaMessageParser;
import com.serotonin.modbus4j.ip.xa.XaMessageRequest;
import com.serotonin.modbus4j.ip.xa.XaWaitingRoomKeyFactory;
import com.serotonin.modbus4j.msg.ModbusRequest;
import com.serotonin.modbus4j.msg.ModbusResponse;
import com.serotonin.modbus4j.sero.messaging.EpollStreamTransport;
import com.serotonin.modbus4j.sero.messaging.MessageControl;
import com.serotonin.modbus4j.sero.messaging.OutgoingRequestMessage;
import com.serotonin.modbus4j.sero.messaging.StreamTransport;
import com.serotonin.modbus4j.sero.messaging.Transport;
import com.serotonin.modbus4j.sero.messaging.WaitingRoomKeyFactory;

public class TcpListener extends ModbusMaster {
    // Configuration fields.
    private short nextTransactionId = 0;
    private short retries = 0;
    private final IpParameters ipParameters;

    // Runtime fields.
    private ServerSocket serverSocket;
    private Socket socket;
    private ExecutorService executorService;
    private ListenerConnectionHandler handler;

    public TcpListener(IpParameters params) {        
        ipParameters = params;
        connected = false;        
    }

    protected short getNextTransactionId() {
        return nextTransactionId++;
    }

    @Override
    synchronized public void init() throws ModbusInitException {        
        executorService = Executors.newCachedThreadPool();
        startListener();
        initialized = true;        
    }

    private void startListener() throws ModbusInitException {
        try {
            handler = new ListenerConnectionHandler(socket);
            executorService.execute(handler);
        }
        catch (Exception e) {
            throw new ModbusInitException(e);
        }
    }

    @Override
    synchronized public void destroy() {
        // Close the serverSocket first to prevent new messages.
        try {
            if (serverSocket != null)
                serverSocket.close();
        }
        catch (IOException e) {           
            getExceptionHandler().receivedException(e);
        }

        // Close all open connections.
        if (handler != null) {
            handler.closeConnection();
        }

        // Terminate Listener
        terminateListener();
        initialized = false;      
    }

    private void terminateListener() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(300, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            getExceptionHandler().receivedException(e);
        }
        handler = null;
    }

    @Override
    synchronized public ModbusResponse sendImpl(ModbusRequest request) throws ModbusTransportException {

        if (!connected) {
            throw new ModbusTransportException(new Exception("TCP Listener has no active connection!"),
                    request.getSlaveId());
        }

        if (!initialized) {
            return null;
        }

        // Wrap the modbus request in a ip request.
        OutgoingRequestMessage ipRequest;
        if (ipParameters.isEncapsulated()) {
            ipRequest = new EncapMessageRequest(request);
            StringBuilder sb = new StringBuilder();
            for (byte b : Arrays.copyOfRange(ipRequest.getMessageData(), 0, ipRequest.getMessageData().length)) {
                sb.append(String.format("%02X ", b));
            }
        }
        else {
            ipRequest = new XaMessageRequest(request, getNextTransactionId());
            StringBuilder sb = new StringBuilder();
            for (byte b : Arrays.copyOfRange(ipRequest.getMessageData(), 0, ipRequest.getMessageData().length)) {
                sb.append(String.format("%02X ", b));
            }
        }

        // Send the request to get the response.
        IpMessageResponse ipResponse;
        try {
            // Send data via handler!
            handler.conn.DEBUG = true;
            ipResponse = (IpMessageResponse) handler.conn.send(ipRequest);
            if (ipResponse == null) {
                throw new ModbusTransportException(new Exception("No valid response from slave!"), request.getSlaveId());
            }
            StringBuilder sb = new StringBuilder();
            for (byte b : Arrays.copyOfRange(ipResponse.getMessageData(), 0, ipResponse.getMessageData().length)) {
                sb.append(String.format("%02X ", b));
            }
            return ipResponse.getModbusResponse();
        }
        catch (Exception e) {
            if (retries < 10 && !e.getLocalizedMessage().contains("Broken")) {
                retries++;
            }
            else {
                /*
                 * To recover from a Broken Pipe, the only way is to restart serverSocket
                 */

                // Close the serverSocket first to prevent new messages.
                try {
                    if (serverSocket != null)
                        serverSocket.close();
                }
                catch (IOException e2) {
                    getExceptionHandler().receivedException(e2);
                }

                // Close all open connections.
                if (handler != null) {
                    handler.closeConnection();
                    terminateListener();
                }

                if (!initialized) {
                    return null;
                }

                executorService = Executors.newCachedThreadPool();
                try {
                    startListener();
                }
                catch (Exception e2) {
                    throw new ModbusTransportException(e2, request.getSlaveId());
                }
                retries = 0;
            }
            // Simple send error!
            throw new ModbusTransportException(e, request.getSlaveId());
        }
    }

    class ListenerConnectionHandler implements Runnable {
        private Socket socket;
        private Transport transport;
        private MessageControl conn;
        private BaseMessageParser ipMessageParser;
        private WaitingRoomKeyFactory waitingRoomKeyFactory;

        public ListenerConnectionHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {           
            if (ipParameters.isEncapsulated()) {
                ipMessageParser = new EncapMessageParser(true);
                waitingRoomKeyFactory = new EncapWaitingRoomKeyFactory();
            }
            else {
                ipMessageParser = new XaMessageParser(true);
                waitingRoomKeyFactory = new XaWaitingRoomKeyFactory();
            }

            try {
                acceptConnection();
            }
            catch (IOException e) {
                conn.close();
                closeConnection();
                getExceptionHandler().receivedException(new ModbusInitException(e));
            }
        }

        private void acceptConnection() throws IOException, BindException {
            while (true) {
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (!connected) {
                    try {
                        serverSocket = new ServerSocket(ipParameters.getPort());
                        socket = serverSocket.accept();

                        if (getePoll() != null)
                            transport = new EpollStreamTransport(socket.getInputStream(), socket.getOutputStream(),
                                    getePoll());
                        else
                            transport = new StreamTransport(socket.getInputStream(), socket.getOutputStream());
                        break;
                    }
                    catch (Exception e) {
                        if (e instanceof SocketTimeoutException) {
                            continue;
                        }
                        else if (e.getLocalizedMessage().contains("closed")) {
                            return;
                        }
                        else if (e instanceof BindException) {
                            closeConnection();
                            throw (BindException) e;
                        }
                    }
                }
            }

            conn = getMessageControl();
            conn.setExceptionHandler(getExceptionHandler());
            conn.DEBUG = true;
            conn.start(transport, ipMessageParser, null, waitingRoomKeyFactory);
            if (getePoll() == null)
                ((StreamTransport) transport).start("Modbus4J TcpMaster");
            connected = true;
        }

        void closeConnection() {
            if (conn != null) {
                closeMessageControl(conn);
            }

            try {
                if (socket != null) {
                    socket.close();
                }
            }
            catch (IOException e) {
                getExceptionHandler().receivedException(new ModbusInitException(e));
            }
            connected = false;
            conn = null;
            socket = null;
        }
    }
}
