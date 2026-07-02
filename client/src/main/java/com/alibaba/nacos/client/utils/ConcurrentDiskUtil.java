/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * 并发磁盘 I/O 工具类 —— 基于 Java NIO {@link FileChannel} 和
 * {@link FileLock} 实现线程安全、进程安全的文件读写操作。
 *
 * <h2>核心特性</h2>
 * <ul>
 *   <li><b>文件锁（FileLock）</b>：通过 {@code tryLock()} 获取排他锁/共享锁，
 *       防止并发写冲突。锁获取失败时最多重试 10 次（递增退避）</li>
 *   <li><b>NIO 缓冲</b>：使用 {@link ByteBuffer} 和 {@link FileChannel}
 *       实现高效字节级读写</li>
 *   <li><b>原子写入</b>：写入后调用 {@code channel.truncate()} 确保
 *       文件长度与内容一致（防止旧内容残留）</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <p>主要用于 {@link com.alibaba.nacos.client.naming.cache.DiskCache}
 * 的磁盘缓存文件读写，确保多线程/多进程环境下服务列表缓存的完整性。</p>
 *
 * @author nkorange
 */
public class ConcurrentDiskUtil {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentDiskUtil.class);
    
    private static final String READ_ONLY = "r";
    
    private static final String READ_WRITE = "rw";
    
    private static final int RETRY_COUNT = 10;
    
    private static final int SLEEP_BASETIME = 10;
    
    /**
     * get file content.
     *
     * @param path        file path
     * @param charsetName charsetName
     * @return content
     * @throws IOException IOException
     */
    public static String getFileContent(String path, String charsetName) throws IOException {
        File file = new File(path);
        return getFileContent(file, charsetName);
    }
    
    /**
     * get file content.
     *
     * @param file        file
     * @param charsetName charsetName
     * @return content
     * @throws IOException IOException
     */
    public static String getFileContent(File file, String charsetName) throws IOException {
        try (RandomAccessFile fis = new RandomAccessFile(file, READ_ONLY);
            FileChannel fcin = fis.getChannel();
            FileLock rlock = tryLock(file, fcin, true)) {
            int fileSize = (int) fcin.size();
            ByteBuffer byteBuffer = ByteBuffer.allocate(fileSize);
            fcin.read(byteBuffer);
            byteBuffer.flip();
            return byteBufferToString(byteBuffer, charsetName);
        }
    }
    
    /**
     * write file content.
     *
     * @param path        file path
     * @param content     content
     * @param charsetName charsetName
     * @return whether write ok
     * @throws IOException IOException
     */
    public static Boolean writeFileContent(String path, String content, String charsetName)
        throws IOException {
        File file = new File(path);
        return writeFileContent(file, content, charsetName);
    }
    
    /**
     * write file content.
     *
     * @param file        file
     * @param content     content
     * @param charsetName charsetName
     * @return whether write ok
     * @throws IOException IOException
     */
    public static Boolean writeFileContent(File file, String content, String charsetName)
        throws IOException {
        
        if (!file.exists() && !file.createNewFile()) {
            return false;
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, READ_WRITE);
            FileChannel channel = raf.getChannel();
            FileLock lock = tryLock(file, channel, false)) {
            byte[] contentBytes = content.getBytes(charsetName);
            ByteBuffer sendBuffer = ByteBuffer.wrap(contentBytes);
            while (sendBuffer.hasRemaining()) {
                channel.write(sendBuffer);
            }
            channel.truncate(contentBytes.length);
        } catch (FileNotFoundException e) {
            throw new IOException("file not exist");
        }
        return true;
    }
    
    /**
     * transfer ByteBuffer to String.
     *
     * @param buffer      buffer
     * @param charsetName charsetName
     * @return String
     * @throws IOException IOException
     */
    public static String byteBufferToString(ByteBuffer buffer, String charsetName)
        throws IOException {
        Charset charset = Charset.forName(charsetName);
        CharsetDecoder decoder = charset.newDecoder();
        CharBuffer charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
        return charBuffer.toString();
    }
    
    private static void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            LOGGER.warn("sleep wrong", e);
            // set the interrupted flag
            Thread.currentThread().interrupt();
        }
    }
    
    private static FileLock tryLock(File file, FileChannel channel, boolean shared)
        throws IOException {
        FileLock result = null;
        int i = 0;
        do {
            try {
                result = channel.tryLock(0L, Long.MAX_VALUE, shared);
            } catch (Exception e) {
                ++i;
                if (i > RETRY_COUNT) {
                    LOGGER.error("[NA] lock " + file.getName() + " fail;retryed time: " + i, e);
                    throw new IOException("lock " + file.getAbsolutePath() + " conflict");
                }
                sleep(SLEEP_BASETIME * i);
                LOGGER.warn("lock " + file.getName() + " conflict;retry time: " + i);
            }
        } while (null == result);
        return result;
    }
    
}
