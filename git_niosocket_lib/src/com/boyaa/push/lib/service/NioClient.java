package com.boyaa.push.lib.service;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Iterator;

import android.content.Context;
import android.util.Log;

/**
 * 
 * @author Administrator
 *
 */
public class NioClient {
	
	private final int STATE_OPEN=1;//socket打开
	private final int STATE_CLOSE=1<<1;//socket关闭
	private final int STATE_CONNECT_START=1<<2;//开始连接server
	private final int STATE_CONNECT_SUCCESS=1<<3;//连接成功
	private final int STATE_CONNECT_FAILED=1<<4;//连接失败
	private final int STATE_CONNECT_WAIT=1<<5;//等待连接
	
	private String IP="192.168.1.101";
	private int PORT=60000;

	private Selector selector;
	private ByteBuffer readBuffer = ByteBuffer.allocate(8192);
	
	
	private int state=STATE_CONNECT_START;
	
	private Thread conn=null;
	private Thread rec=null;
	
	private Context context;
	private ISocketResponse respListener;
	private ArrayList<Packet> requestQueen=new ArrayList<Packet>();
	private final Object lock=new Object();
	private final String TAG="Client";
	
	public int send(Packet in)
	{
		synchronized (lock) 
		{
			requestQueen.add(in);
		}
		this.selector.wakeup();
		return in.getId();
	}
	
	public void cancel(int reqId)
	{
		 Iterator<Packet> mIterator=requestQueen.iterator();
		 while (mIterator.hasNext()) 
		 {
			 Packet packet=mIterator.next();
			 if(packet.getId()==reqId)
			 {
				 mIterator.remove();
			 }
		}
	}
	
	public NioClient(Context context,ISocketResponse respListener)
	{
		this.context=context;
		this.respListener=respListener;
	}
	
	public boolean isSocketConnected()
	{
		return ((state==STATE_CONNECT_SUCCESS)&&(null!=rec&&rec.isAlive()));
	}
	
	public void open()
	{
		reconn();
	}
	
	public void open(String host,int port)
	{
		this.IP=host;
		this.PORT=port;
		reconn();
	}
	
	private long lastConnTime=0;
	public synchronized void reconn()
	{
		if(System.currentTimeMillis()-lastConnTime<2000)
		{
			return;
		}
		lastConnTime=System.currentTimeMillis();
		
		close();
		state=STATE_OPEN;
		conn=new Thread(new Conn());
		conn.start();
	}
	
	public synchronized void close()
	{
		try {
			if(state!=STATE_CLOSE)
			{
				try {
					if(null!=conn&&conn.isAlive())
					{
						conn.interrupt();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					conn=null;
				}
				
				try {
					if(null!=rec&&rec.isAlive())
					{
						rec.interrupt();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					rec=null;
				}
				
				state=STATE_CLOSE;
			}
			requestQueen.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private class Conn implements Runnable
	{
		public void run() {
Log.v(TAG,"Conn :Start");
			try {
					state=STATE_CONNECT_START;
					
					InetSocketAddress address=new InetSocketAddress(IP, PORT);
					selector=SelectorProvider.provider().openSelector();
					SocketChannel socketChannel = SocketChannel.open();
					socketChannel.configureBlocking(false);
					socketChannel.connect(address);
					socketChannel.register(selector, SelectionKey.OP_CONNECT);
					
					while(state!=STATE_CLOSE)
					{
						try {
								selector.select();
								Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
								while (selectedKeys.hasNext())
								{
									SelectionKey key = (SelectionKey) selectedKeys.next();
									selectedKeys.remove();

									if (!key.isValid())
									{
										continue;
									}

									if (key.isConnectable()) 
									{
										finishConnection(key);
									} 
									else if (key.isReadable()) 
									{
										read(key);
									} 
									else if (key.isWritable())
									{
										write(key);
									}
								}
								
								synchronized(lock)
								{
									if(requestQueen.size()>0)
									{
										SelectionKey  key=socketChannel.keyFor(selector);
										key.interestOps(SelectionKey.OP_WRITE);
									}
								}
								state=STATE_CONNECT_SUCCESS;
						} catch (Exception e) {
							e.printStackTrace();
							state=STATE_CONNECT_FAILED;
						}
					}
			} catch (Exception e) {
				e.printStackTrace();
			}

Log.v(TAG,"Conn :End");
		}
		
		
		private void finishConnection(SelectionKey key) throws IOException {
			SocketChannel socketChannel = (SocketChannel) key.channel();
		
			// Finish the connection. If the connection operation failed
			// this will raise an IOException.
			try {
				socketChannel.finishConnect();
			} catch (IOException e) {
				// Cancel the channel's registration with our selector
Log.v("NioSocket", "finishConnection:"+e.getMessage());
				key.cancel();
				return;
			}
		
			// Register an interest in writing on this channel
			key.interestOps(SelectionKey.OP_READ);
		}
		
		
		private void read(SelectionKey key) throws IOException {
			SocketChannel socketChannel = (SocketChannel) key.channel();

			// Clear out our read buffer so it's ready for new data
			readBuffer.clear();

			// Attempt to read off the channel
			int numRead;
			try {
				numRead = socketChannel.read(readBuffer);
			} catch (IOException e) {
				// The remote forcibly closed the connection, cancel
				// the selection key and close the channel.
				key.cancel();
				socketChannel.close();
				return;
			}

			if (numRead == -1) {
				// Remote entity shut the socket down cleanly. Do the
				// same from our end and cancel the channel.
				key.channel().close();
				key.cancel();
				return;
			}
respListener.onSocketResponse(new String(readBuffer.array(), 0, numRead));
		}
		
		private void write(SelectionKey key) throws IOException 
		{
			SocketChannel socketChannel = (SocketChannel) key.channel();
			
			synchronized (lock) 
			{
				 Packet item;
				 Iterator<Packet> iter=requestQueen.iterator();
				 while(iter.hasNext())
				 {
					 item=iter.next();
					 ByteBuffer buf=ByteBuffer.wrap(item.getPacket());
					 socketChannel.write(buf);
					 iter.remove();
				 }
				 item=null;
			}

			
			key.interestOps(SelectionKey.OP_READ);
		}
	}
}
