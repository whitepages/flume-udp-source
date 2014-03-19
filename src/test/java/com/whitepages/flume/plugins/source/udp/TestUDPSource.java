/*
 * This file is distributed under the same license as Apache Flume itself.
 *   http://www.apache.org/licenses/LICENSE-2.0
 * See the NOTICE file for copyright information.
 */
package com.whitepages.flume.plugins.source.udp;

import java.util.ArrayList;
import java.util.List;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.MulticastSocket;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;

public class TestUDPSource {
  private UDPSource source;
  private Channel   channel;

  private static final String UNICAST_HOST   = "127.0.0.1";
  private static final int    UNICAST_PORT   = 14453;

  private static final String MULTICAST_HOST = "224.1.0.1";
  private static final int    MULTICAST_PORT = 14454;

  private static final String testMessage    = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

  @Before
  public void setUp() {
    source  = new UDPSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
  }

  @Test
  public void testUnicast() throws Exception {
    Event          event       = null;
    DatagramSocket socket      = null;
    InetAddress    address     = null;

    // configure the UDPSource
    Context context = new Context();
    context.put("host", UNICAST_HOST);
    context.put("port", String.valueOf(UNICAST_PORT));
    source.configure(context);

    // initialize sockets and packet to send.
    socket  = new DatagramSocket();
    address = InetAddress.getByName(UNICAST_HOST);
    String testMessageWithNewline = testMessage + "\n";
    byte[] sendData = testMessageWithNewline.getBytes();
    DatagramPacket packet = new DatagramPacket(sendData, sendData.length, address, UNICAST_PORT);

    // start the UDPSource
    source.start();

    // send the packet.
    socket.send(packet);

    // read the packet from the channel the UDPSource is part of.
    Transaction txn = channel.getTransaction();
    try {
      txn.begin();
      event = channel.take();
      txn.commit();
    } finally {
      txn.close();
    }

    // stop the source
    source.stop();

    // assert that the packet was read correctly.
    Assert.assertNotNull(event);
    Assert.assertArrayEquals(testMessage.getBytes(), event.getBody());
  }

  @Test
  public void testMulticast() throws Exception {
    Event           event = null;
    MulticastSocket socket = null;
    InetAddress     address = null;

    // configure UDPSource for multicast
    Context context = new Context();
    context.put("host", MULTICAST_HOST);
    context.put("port", String.valueOf(MULTICAST_PORT));
    context.put("multicast", "true");
    source.configure(context);

    // initialize sockets and packet to send.
    socket  = new MulticastSocket();
    address = InetAddress.getByName(MULTICAST_HOST);
    String testMessageWithNewline = testMessage + "\n";
    byte[] sendData = testMessageWithNewline.getBytes();
    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, MULTICAST_PORT);

    // Start the multicast UDPSource
    source.start();

    // send the packet
    socket.send(sendPacket);

    // read the packet from the channel the UDPSource is part of.
    Transaction txn = channel.getTransaction();
    try {
      txn.begin();
      event = channel.take();
      txn.commit();
    } finally {
      txn.close();
    }

    // Stop the multicast UDPSource
    source.stop();

    // assert that the packet was read correctly.
    Assert.assertNotNull(event);
    Assert.assertArrayEquals(testMessage.getBytes(), event.getBody());
  }
}
