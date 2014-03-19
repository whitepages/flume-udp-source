/*
 * This file is distributed under the same license as Apache Flume itself.
 *   http://www.apache.org/licenses/LICENSE-2.0
 * See the NOTICE file for copyright information.
 */

/*
 * Note:  Much of this class was taken from the SyslogUDPSource.  It
 * has been striped of Syslog specific code, and enhanced to allow
 * to allow for joining multicast groups.
 *
 * TODO:  Abstract this into a more generic UDPSource and make SyslogUDPSource
 * a subclass. (?)
 */

package com.whitepages.flume.plugins.source.udp;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;

import java.net.UnknownHostException;
import java.net.SocketException;

import java.io.ByteArrayOutputStream;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDPSource extends AbstractSource implements EventDrivenSource, Configurable {

  // configuratable attributes.
  // Defaults defined in UDPSourceConfigurationConstants.java
  private String    host;
  private int       port;
  private char      delimiter;
  private boolean   multicast;
  private String    multicastInterface;
  private int       maxSize;

  private DatagramChannel nettyChannel;

  private static final Logger logger = LoggerFactory.getLogger(UDPSource.class);

  private CounterGroup counterGroup = new CounterGroup();


  @Override
  public void start() {
    // setup Netty server
    DatagramChannelFactory datagramChannelFactory =
      new OioDatagramChannelFactory(Executors.newCachedThreadPool());

    ConnectionlessBootstrap serverBootstrap =
      new ConnectionlessBootstrap(datagramChannelFactory);

    final UDPChannelHandler handler = new UDPChannelHandler(maxSize, delimiter);

    serverBootstrap.setOption("recieveBufferSize", 65536);
    serverBootstrap.setOption("receiveBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(65536));

    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() {
        return Channels.pipeline(handler);
      }
    });

    nettyChannel = (DatagramChannel)serverBootstrap.bind(new InetSocketAddress(host, port));

    if (multicast) {
      try {
        nettyChannel.joinGroup(
          new InetSocketAddress(host, port),
          NetworkInterface.getByName(multicastInterface)
        );
      } catch (Exception e) {
        logger.error("Could not join multicast group " + host + " on interface " +
          multicastInterface + ": " + e.getMessage());
        throw new FlumeException(e);
      }
    }

    super.start();
  }

  @Override
  public void stop() {
    logger.info("UDP Source stopping...");
    logger.info("Metrics:{}", counterGroup);
    if (nettyChannel != null) {
      if (multicast) {
        try {
          nettyChannel.leaveGroup(InetAddress.getByName(host));
        } catch (UnknownHostException e) {
          logger.warn("Did not properly leave multicast group " + host +
            " when stopping down UDPSource: " + e.getMessage());
        }
      }
      nettyChannel.close();
      try {
        nettyChannel.getCloseFuture().await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("netty server stop interrupted", e);
      } finally {
        nettyChannel = null;
      }
    }

    super.stop();
  }

  @Override
  public void configure(Context context) {
    // Port cannot be null
    Configurables.ensureRequiredNonNull(context, UDPSourceConfigurationConstants.CONFIG_PORT);

    // get host, default is 0.0.0.0
    host = context.getString(UDPSourceConfigurationConstants.CONFIG_HOST,
      UDPSourceConfigurationConstants.DEFAULT_HOST);
    port = context.getInteger(UDPSourceConfigurationConstants.CONFIG_PORT);

    // get delimiter, default is '\n'
    delimiter = context.getString(UDPSourceConfigurationConstants.CONFIG_DELIMITER,
      UDPSourceConfigurationConstants.DEFAULT_DELIMITER).charAt(0);

    // Should we bind and join multicast? Default is false.
    multicast = context.getBoolean(UDPSourceConfigurationConstants.CONFIG_MULTICAST,
      UDPSourceConfigurationConstants.DEFAULT_MULTICAST);

    // get interface, default is eth0
    multicastInterface = context.getString(UDPSourceConfigurationConstants.CONFIG_INTERFACE,
      UDPSourceConfigurationConstants.DEFAULT_INTERFACE);

    // max packet size
    maxSize = context.getInteger(UDPSourceConfigurationConstants.CONFIG_MAXSIZE,
      UDPSourceConfigurationConstants.DEFAULT_MAXSIZE);
  }


  // class to handle incoming Netty message events.
  public class UDPChannelHandler extends SimpleChannelHandler {
    protected int maxSize;
    protected char delimiter;
    protected boolean isIncompleteEvent;
    protected ByteArrayOutputStream baos;

    public UDPChannelHandler(int maxSize, char delimiter) {
      super();

      this.maxSize           = maxSize;
      this.delimiter         = delimiter;
      this.isIncompleteEvent = false;

      this.baos              = new ByteArrayOutputStream(this.maxSize);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
      try {
        Event e = extractEvent((ChannelBuffer)mEvent.getMessage());
        if (e == null) {
          return;
        }
        getChannelProcessor().processEvent(e);
        counterGroup.incrementAndGet("events.success");
      } catch (ChannelException ex) {
        counterGroup.incrementAndGet("events.dropped");
        logger.error("Error writting to channel", ex);
        return;
      }
    }



    // extract bytes DatagramStream needed for building Flume event.
    // Events are split on delimiter, or by maxSize bytes.
    public Event extractEvent(ChannelBuffer in) {

      /* for protocol debugging
      ByteBuffer bb = in.toByteBuffer();
      int remaining = bb.remaining();
      byte[] buf = new byte[remaining];
      bb.get(buf);
      HexDump.dump(buf, 0, System.out, 0);
      */

      byte    b = 0;
      Event   e = null;
      boolean doneReading = false;

      try {
        while (!doneReading && in.readable()) {
          b = in.readByte();

          if (b == delimiter) {
            e = buildEvent();
            doneReading = true;
          }
          else {
            baos.write(b);
          }

          if(baos.size() == this.maxSize && !doneReading){
            isIncompleteEvent = true;
            e = buildEvent();
            doneReading = true;
          }
        }

        if (e == null) {
          // The event closed before we got to the delimiter.
          // build an Event with everything we've read so far.
          doneReading = true;
          e = buildEvent();
        }
        // catch (IndexOutOfBoundsException eF)
        //    e = buildEvent();
        // TODO: do something useful?
      } catch (Exception e1) {
        //no op
      } finally {
        // no-op
      }

      return e;
    }

    // create the event
    Event buildEvent() {
      byte[] body;

      if (isIncompleteEvent) {
        logger.warn("Event size larger than specified event size: {}. You should " +
          "consider increasing your event size.", maxSize);
      }

      body = baos.toByteArray();

      reset();
      // format the message
      return EventBuilder.withBody(body);
    }

    private void reset(){
      baos.reset();
      isIncompleteEvent = false;
    }
  }
}
