import java.io.*;
import java.util.*;

public class FreeList {
    private static List<Integer> freePositions = new ArrayList<>();

    public static void addFreePosition(int position, String FILE_PATH) {
        freePositions.add(position);
        saveFreeListToFile(FILE_PATH);
    }

    public static int getFreePosition(String FILE_PATH) {
        if (freePositions.isEmpty()) {
            return -1;
        } else {
            int position = freePositions.remove(freePositions.size() - 1);
            saveFreeListToFile(FILE_PATH);
            return position;
        }
    }

    public static boolean hasFreePosition() {
        return !freePositions.isEmpty();
    }

    public static void saveFreeListToFile(String FILE_PATH) {
        try (RandomAccessFile file = new RandomAccessFile(FILE_PATH, "rw")) {
            file.seek(0);
            // free list 빈 레코드 위치 정보 파일에 저장
            for (int position : freePositions) {
                file.writeInt(position);
            }
        } catch (IOException e) {
            System.out.println("Free List 저장에 실패했습니다.");
            e.printStackTrace();
        }
    }

    // 파일 첫 부분의 free list 정보를 읽어와서 freePositions에 저장
    public static void loadFreeListFromFile(String FILE_PATH, int data_file_size) {
        try (RandomAccessFile file = new RandomAccessFile(FILE_PATH, "r")) {
            file.seek(0);
            freePositions.clear();
            // 레코드 크기의 free list를 확인하여 position 정보 삽입
            while (file.getFilePointer() < data_file_size) {
                int position = file.readInt();
                if (position != -1) {
                    freePositions.add(position);
                }
            }
        } catch (IOException e) {
            System.out.println("Free List 로드에 실패했습니다.");
            e.printStackTrace();
        }
    }
}