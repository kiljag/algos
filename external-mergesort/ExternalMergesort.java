import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class ExternalMergesort {

    private static final int MEMORY_SIZE = 500 * 1024 * 1024; // can hold this many bytes
    private static final int LOG_FACTOR = 8; // log is 4 times that of memory
    private static final String FILE_PATH = "/tmp/integer.bin";
    private static final String FILE_PATH_SORTED = "/tmp/integer_sorted.bin";

    static int randomInt() {
//        return Double.valueOf(Math.random() * Integer.MAX_VALUE).intValue();
        return Double.valueOf(Math.random() * 1000).intValue();
    }

    static void createNewFile(String filePath) throws Exception {
        Path path = Path.of(filePath);
        Files.deleteIfExists(path);
        Files.createFile(path);
    }

    static int readInteger(byte[] buffer, int off) {
        int s1 = (buffer[off] & 0xff) << 24;
        int s2 = (buffer[off+1] & 0xff) << 16;
        int s3 = (buffer[off+2] & 0xff) << 8;
        int s4 = (buffer[off+3] & 0xff);
        return s1 | s2 | s3 | s4;
    }

    static void writeInteger(byte[] buffer, int off, int ri) {
        buffer[off] = (byte) (ri >> 24);
        buffer[off+1] = (byte) (ri >> 16);
        buffer[off+2] = (byte) (ri >> 8);
        buffer[off+3] = (byte) ri;
    }

    static void createIntegerFile() throws Exception {

        RandomAccessFile raf = new RandomAccessFile(FILE_PATH, "rw");


        byte[] buffer = new byte[MEMORY_SIZE];
        for (int li = 0; li < LOG_FACTOR; li++) {

            // populate the memory buffer with random integers
            for (int off = 0; off < MEMORY_SIZE; off += 4) {
                int ri = randomInt();
                writeInteger(buffer, off, ri);
//                System.out.println("rand : " + ri);
            }
            raf.seek((long) li * MEMORY_SIZE);
            raf.write(buffer);
        }

        raf.close();
    }

    static class ChunkInfo {
        int index;
        long start;
        long length;
        long offset;
        int bufOffset;
        int element;
        byte[] buffer;
        RandomAccessFile raf;

        public ChunkInfo(int index, long start, long length) {
            this.index = index;
            this.start = start;
            this.length = length;
            this.offset = 0;
        }

        public void initialize(int bufsize) throws Exception {
//            System.out.printf("chunk : start %d, length %d, bufsize : %d\n", start, length, bufsize);
            buffer = new byte[bufsize];
            bufOffset = 0;
            raf = new RandomAccessFile(FILE_PATH, "r");
            raf.seek(start);
        }

        public boolean canRead() {
            return offset < length;
        }

        public void next() throws Exception {
//            System.out.printf("next : %d, start %d, length %d, offset : %d, bufoffset : %d \n", index, start, length, offset, bufOffset);
            if (offset < length) {
                if (bufOffset == 0) {
                    int readlen = buffer.length;
                    if (length - offset < readlen) {
                        readlen = (int)(length - offset);
                    }
                    raf.seek(start + offset);
                    raf.readFully(buffer, 0, readlen);
                }
                element = readInteger(buffer, bufOffset);
                bufOffset = (bufOffset + 4) % buffer.length;
                offset += 4;

            } else {
                if (raf != null) {
                    raf.close();
                }
            }
        }
    }

    static void sortIntegerFile() throws Exception{

        RandomAccessFile raf = new RandomAccessFile(FILE_PATH, "rw");
        List<ChunkInfo> chunks = new ArrayList<>();

        System.out.println("File size : " + raf.length());
        byte[] buffer = new byte[MEMORY_SIZE/2];
        int[] arr = new int[MEMORY_SIZE/8];
        long offset = 0;

        while (offset < raf.length()) {
//            System.out.println("offset  :" + offset);
            raf.seek(offset);
            int readlen = buffer.length;
            if (readlen > raf.length() - offset) {
                readlen = (int) (raf.length() - offset);
            }
            raf.readFully(buffer, 0, readlen);
            int numInts = readlen/4;
            for (int i = 0; i < numInts; i++) {
                arr[i] = readInteger(buffer, i * 4);
            }
            Arrays.sort(arr);
            for (int i = 0; i < numInts; i++) {
//                System.out.println("chunk sort : " + arr[i]);
                writeInteger(buffer, i * 4, arr[i]);
            }

            raf.seek(offset);
            raf.write(buffer, 0, numInts * 4);

            chunks.add(new ChunkInfo(chunks.size(), offset, 4 * numInts));
            offset += numInts * 4;
        }

        raf.close();
        System.out.println("chunks : " + chunks.size());


        // merge k sorted lists
        PriorityQueue<ChunkInfo> pq = new PriorityQueue<>(Comparator.comparingInt(a -> a.element));
        RandomAccessFile sraf = new RandomAccessFile(FILE_PATH_SORTED, "rw");

        int memChunkSize = ((int)(0.8 * MEMORY_SIZE)) / chunks.size();
        memChunkSize = (memChunkSize / 4) * 4;
        System.out.println("Memory chunk size : " + memChunkSize);
        for (ChunkInfo chunk : chunks) {
            chunk.initialize(memChunkSize);
            chunk.next();
            pq.add(chunk);
        }

        int buffSize = (int)(0.8 * MEMORY_SIZE);
        buffSize = (buffSize / 4) * 4;
        System.out.println("Sorted buffer size : " + buffSize);
        byte[] sortedBuffer = new byte[buffSize];
        int buffOffset = 0;

        while (!pq.isEmpty()) {
            ChunkInfo top = pq.poll();
//            System.out.printf("index : %d, element : %d\n", top.index, top.element);
            writeInteger(sortedBuffer, buffOffset, top.element);
            buffOffset += 4;

            if (buffOffset >= sortedBuffer.length) {
                sraf.write(sortedBuffer, 0, buffOffset);
                buffOffset = 0;
            }

            if (top.canRead()) {
                top.next();
                pq.add(top);
            }
        }

        if (buffOffset > 0) {
            sraf.write(sortedBuffer, 0, buffOffset);
        }
        sraf.close();
    }

    public static void checkSorted() throws Exception {

        RandomAccessFile raf = new RandomAccessFile(FILE_PATH_SORTED, "rw");
        int prev = Integer.MIN_VALUE;
        long offset = 0;
        byte[] buffer = new byte[MEMORY_SIZE/2];
        boolean isSorted = true;
        while (offset < raf.length()) {
            raf.seek(offset);
            int n = raf.read(buffer, 0, buffer.length);
            int numInts = n/4;

            for (int i = 0; i < numInts; i++) {
                int curr = readInteger(buffer, i * 4);
//                System.out.println("curr : " + curr);
                if (curr < prev) {
                    isSorted = false;
                } else {
                    prev = curr;
                }
            }

            offset += numInts * 4;
        }
        if (isSorted) {
            System.out.println("numbers are sorted");
        } else {
            System.out.println("number are not sorted");
        }

        raf.close();
    }

    public static void main(String[] args) {
        try {

            long start, end;

            createNewFile(FILE_PATH);
            createNewFile(FILE_PATH_SORTED);

            System.out.println("memory size : " + MEMORY_SIZE);

            System.out.println("Creating integer file..");
            start = System.currentTimeMillis();
            createIntegerFile();
            end = System.currentTimeMillis();
            System.out.println("Time taken (sec) : " + (end-start)/1000.0);


            System.out.println("Sorting..");
            start = System.currentTimeMillis();
            sortIntegerFile();
            end = System.currentTimeMillis();
            System.out.println("Time taken (sec) : " + (end-start)/1000.0);

            System.out.println("Checking..");
            start = System.currentTimeMillis();
            checkSorted();
            end = System.currentTimeMillis();
            System.out.println("Time taken (sec) : " + (end-start)/1000.0);

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception : " + e);
        }
    }
}
