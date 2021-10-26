package io.openmessaging;

import io.openmessaging.engine.db.Slices;
import io.openmessaging.engine.include.Slice;
import io.openmessaging.engine.table.BlockEntry;
import io.openmessaging.engine.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {


    private Logger logger = LoggerFactory.getLogger(MessageQueue.class);

    public ConcurrentHashMap<String,Map<Integer,Map<Long,ByteBuffer>>> appendData = new ConcurrentHashMap<>();

    //保存该topic-queueid下的最大位点offset集合
    Map<Integer,Long> topicOffset = new ConcurrentHashMap<>();

    //基于页内存刷写内存到磁盘
    protected MappedByteBuffer mappedByteBuffer;

    protected FileChannel fileChannel;

    //记录追加写入的数据量当前位置
    protected AtomicInteger wrotePosition = new AtomicInteger(0);

    //用同步锁的进行线程同步
    Lock lock = new ReentrantLock();

    /**保存刷写到磁盘之前小文件的位点 **/
    String fileName;//文件名
    int fileSize; //文件大小
    File file;//文件对象的指针


    public static final int OS_PAGE_SIZE = 1024*4;
    public static final int BLOCK_SIZE = 1024*64;


    protected File init(String fileName,int fileSize) throws IOException {
        try {
            this.file = new File(fileName);
            this.fileSize = fileSize;
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        }catch (IOException ex){
            logger.error("Failed to create file : {},Exception: {}" , this.fileName, ex);
        }finally {
            if (this.fileChannel!=null){
                this.fileChannel.close();
            }
        }
        return file;

    }

    /**
     * 一个topic+queueId对应一个最大消费位点
     * @param map
     * @param key queueId
     * @param defaultValue offset
     * @return
     */
    private BlockEntry getOrPutDefault(Map<Slice, Slice> map, Integer key, Long defaultValue){
        Slice retObj = map.get(key);

        Slice keySlice = Slices.wrappedBuffer(TypeTransFrom.intToByteArray(key));
        Slice valueSlice = Slices.wrappedBuffer(TypeTransFrom.longToBytes(defaultValue));
        BlockEntry blockEntry = createBlockEntry(keySlice,valueSlice);
        if(retObj != null){
            return blockEntry;
        }
        map.put(keySlice,valueSlice);
        return blockEntry;
    }

    /**
     * 追加写数据和记录消费位点不同
     * 除了需要保持消费位点与下次追加数据的映射关系外
     * 因为MQ是以Topic进行逻辑分组的 所以需要使用FileChannel这样的文件IO从主存加载偏移量文件
     * 以及基于MappedBuffer这样的DirectIO直接从用户进程的虚拟地址映射空间
     * @param map
     * @param topic queueId
     * @param offset offset
     * @return
     */
    public BlockEntry getDefaultOrValue0(Map<String,Map<Integer,Map<Long,ByteBuffer>>> map,String topic,int queueId,long offset0){

        long offset = topicOffset.getOrDefault(topic,offset0);
        //文件名为queueID+offset topic名字为appendData二级缓存的Key值
        //操作系统基础知识 pageSize单位为4kb 磁盘块单位大小64kb
        String fileName1 = topic+String.valueOf(offset);
        try {
            init(fileName1,BLOCK_SIZE);
        }catch (Exception ex){
            logger.info("Error About read file failed... Exception: {}",ex.getCause().getMessage());
        }


        Slice saveSlice = new Slice(0);
        BlockEntry internalBlockEntry = new BlockEntry(new Slice(0),new Slice(0));
        //记录上一个消费位点写入偏移量的大小
        // 操作系统的pageCahe作为三级缓存组成一个ByteBuffer
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        AtomicInteger flush = new AtomicInteger(0);
        for(int i=0,j=0;i<this.fileSize;i += OS_PAGE_SIZE,j++){
            //写入一个PageSize
            byteBuffer.put(i,(byte) 0);
            try {
            //写满一个磁盘块的大小才能写入
            if((i/OS_PAGE_SIZE) - (flush.get()/OS_PAGE_SIZE) >=16){
                int finalI = i;
                flush.getAndUpdate(x -> (x = finalI));
                //先不刷盘
                fileChannel.force(false);

                //保证顺序写磁盘
                lock.lock();

                fileChannel.write(ByteBuffer.wrap(new byte[BLOCK_SIZE]),wrotePosition.getAndSet(BLOCK_SIZE));
                fileChannel.force(true);
                //当一个topic+queueID大于64k磁盘块大小，刷盘后清空文件
                appendData.get(topic).get(queueId).clear();
                lock.unlock();

                }else{
                //FIXME 实现二级缓存
                Map<Long,ByteBuffer> tmp = new ConcurrentSkipListMap<>();
                tmp.getOrDefault(offset,byteBuffer);

                Map<Integer,Map<Long,ByteBuffer>> queueOffsets = new ConcurrentSkipListMap<>();
                queueOffsets.put(queueId,tmp);

                //将每个队列queue收集的offset归纳到一个topic当中
                map.getOrDefault(topic,queueOffsets);
            }

            fileChannel.force(true);
            } catch (IOException e) {
                e.printStackTrace();
            }

            //防止gc
            if(j%1000 == 0){
                try {
                    //让线程让出CPU
                    Thread.sleep(0);
                }catch (Exception ex){
                    logger.error("Interrupted",ex);
                }
            }

        }
        //记录当前queue写入的数据
        internalBlockEntry = new BlockEntry(new Slice(topic),saveSlice);
        internalBlockEntry.topicOffsets = map;
        return internalBlockEntry;

    }

    /**
     * 这里的意思呢
     * 是说我们在写入磁盘的时候 是以磁盘块的形式写入的 即64kb
     * 这边对于一个磁盘块block当中的数据结构可以看作是一个
     * 单向链表，每一个节点代表着即将落盘的每条消息
     * @param key
     * @param value
     * @return
     */
    static BlockEntry createBlockEntry(Slice key, Slice value)
    {
        return new BlockEntry(key,value);
    }


    /**
     * 支持消息落盘时候的顺序写
     * @param topic topic的值，总共有100个topic
     * @param queueId topic下队列的id，每个topic下不超过10000个
     * @param data 信息的内容，评测时会随机产生
     * @return
     */
    @Override
    public long append(String topic, int queueId, ByteBuffer data) throws IOException {

        long offset = topicOffset.getOrDefault(queueId,0L);
        //记录当前topicId-queue
        topicOffset.put(queueId,offset+1);
        init(topic,(int)offset);

        //TODO 底层转换为slice对象 方便在缓冲区内压缩内存
        // 采用跳表的方式 保证顺序写与范围读

        BlockEntry appendData1 = getDefaultOrValue0(appendData,topic,queueId,offset);
        Map<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = appendData1.topicOffsets;
        Map<Long,ByteBuffer> cache = new ConcurrentSkipListMap<Long, ByteBuffer>();
        Slice returnSlice = Slices.allocate(data.remaining());
        cache.put(offset,returnSlice.toByteBuffer());
        Map<Integer, Map<Long, ByteBuffer>> cache1 = new ConcurrentSkipListMap<>();
        cache1.put(queueId,cache);
        appendData.put(topic,cache1);

        return offset;
    }

    /**
     * 支持消息落盘时候的范围读
     * @param topic topic的值
     * @param queueId topic下队列的id
     * @param offset 写入消息时返回的offset
     * @param fetchNum 读取消息个数，不超过100
     * @return
     */
    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> ret = new ConcurrentSkipListMap<>();
        for(int i = 0; i < fetchNum; i++){
            Map<Integer, Map<Long, ByteBuffer>> map1 = appendData.get(topic);
            if(map1 == null){
                break;
            }
            Map<Long, ByteBuffer> m2 = map1.get(queueId);
            if(m2 == null){
                break;
            }
            ByteBuffer buf = m2.get(offset+i);
            if(buf != null){
                // 返回前确保 ByteBuffer 的 remain 区域为完整答案
                buf.position(0);
                buf.limit(buf.capacity());
                ret.put(i,buf);
            }
        }
        return ret;
    }
}
