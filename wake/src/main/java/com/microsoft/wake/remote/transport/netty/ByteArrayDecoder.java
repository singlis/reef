/**
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.remote.transport.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

/**
 * Netty byte array decoder
 */
public class ByteArrayDecoder extends OneToOneDecoder {

  /**
   * Decodes bytes from a channel buffer
   * 
   * @param ctx a channel handler context
   * @param channel a channel
   * @param msg a message
   * @return a byte array
   */
  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg)
      throws Exception {
    if (!(msg instanceof ChannelBuffer)) {
      return msg;
    }
    ChannelBuffer buf = (ChannelBuffer) msg;
    byte[] array;
    if (buf.hasArray()) {
      if (buf.arrayOffset() == 0 && buf.readableBytes() == buf.capacity()) {
        array = buf.array();
      } else {
        array = new byte[buf.readableBytes()];
        buf.getBytes(0, array);
      }
    } else {
      array = new byte[buf.readableBytes()];
      buf.getBytes(0, array);
    }
    
    return array;
  }

  
}
