/*
 * This file is distributed under the same license as Apache Flume itself.
 *   http://www.apache.org/licenses/LICENSE-2.0
 * See the NOTICE file for copyright information.
 */
package com.whitepages.flume.plugins.source.udp;

public final class UDPSourceConfigurationConstants {

  public static final String  CONFIG_PORT       = "port";

  public static final String  CONFIG_HOST       = "host";
  public static final String  DEFAULT_HOST      = "0.0.0.0";

  // Event delimiter.  When character is encountered
  // in the incoming data stream, a new event will be
  // emitted.
  public static final String  CONFIG_DELIMITER  = "delimiter";
  public static final String  DEFAULT_DELIMITER = "\n";

  // If true, the source will join the host as a multicast group.
  public static final String  CONFIG_MULTICAST  = "multicast";
  public static final Boolean DEFAULT_MULTICAST = false;

  // Interface to use for multicast stream.
  // This is irrelevant if multicast is false.
  public static final String  CONFIG_INTERFACE  = "interface";
  public static final String  DEFAULT_INTERFACE = "eth0";


  // Max size of packet before emiting event.  If this much
  // data is read and a delimiter is not encountered,
  // an event will be emitted anyway (and a message will be logged).
  public static final String  CONFIG_MAXSIZE    = "maxsize";
  public static final Integer DEFAULT_MAXSIZE   = 1 << 16; // 64k is max allowable in RFC 5426

  private UDPSourceConfigurationConstants() {
    // Disable explicit creation of objects.
  }

}
