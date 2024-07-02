import java.nio.charset.StandardCharsets;
import java.util.*;
import java.io.*;

// title (30), year (4), length (3), studio_name (30), producer_num(3)
// -> 70bytes (한 레코드 사이즈)
// 구분자 |: 1 byte, enter: 1 byte => 총 75bytes

public class JDBC {
    public static int lastUsedBlockIndex = -1;
    private static String FILE_PATH; // 데이터 파일 경로
    public static final int DATAFILE_SIZE = 131072;
    public static final int BLOCK_SIZE = 256;
    public static final int BUFFER_SIZE = 1024;

    public static int BLOCKING_FACTOR = 0;
    public static int RECORD_SIZE = 0;
    public static int RECORD_CNT = 0;
    public static int attribute_count = 0;
    public static int primary_key_count = 0;

    public static void main(String[] arg){
        System.out.print("(1) 관계 테이블 생성\n" +
                           "(2) 튜플 삽입\n" +
                           "(3) 튜플 삭제\n" +
                           "(4) 튜플 검색\n" +
                           "(5) 해시 조인\n" +
                           "(6) 종료\n"     +
                           "번호를 입력하세요: ");
        Scanner scanner = new Scanner(System.in);
        int choice = Integer.parseInt(scanner.nextLine());
        switch (choice) {
            case 1 -> createRelationTable();
            case 2 -> tupleInsert();
            case 3 -> tupleDelete();
            case 4 -> tupleQuery();
            case 5 -> hashJoin();
            case 6 -> {
                System.out.println("프로그램을 종료합니다.");
            }
            default -> System.out.println("잘못된 선택입니다. 다시 선택해주세요");
        }
        scanner.close();

    }

    public static void hashJoin() {
        Scanner scanner = new Scanner(System.in);
        // join 처리 할 테이블과 속성 입력받음
        System.out.println("해시 조인을 수행할 첫 번째 테이블 이름을 입력하세요:");
        String table1 = scanner.nextLine();
        System.out.println("속성을 입력하세요:");
        String joinKey1 = scanner.nextLine();
        System.out.println("해시 조인을 수행할 두 번째 테이블 이름을 입력하세요:");
        String table2 = scanner.nextLine();
        System.out.println("속성을 입력하세요:");
        String joinKey2 = scanner.nextLine();
        int table1_record_size = 0;
        int table2_record_size = 0;
        int list_size_1 = 0;
        int list_size_2 = 0;

        // 테이블이 존재하는지 확인
        if (Metadata.searchRelationMetadata(table1) && Metadata.searchRelationMetadata(table2)) {
            // 입력받은 속성의 위치를 반환
            Map<String, Integer> positionMap1 = Metadata.getPosition(table1);
            Map<String, Integer> positionMap2 = Metadata.getPosition(table2);
            int joinKeyPos1 = positionMap1.get(joinKey1);
            int joinKeyPos2 = positionMap2.get(joinKey2);

            // 첫 번째 테이블의 blocking factor
            for (Map.Entry<String, Integer> entry : positionMap1.entrySet()) {
                String attribute_name_1 = entry.getKey();
                table1_record_size += Metadata.getLength(table1, attribute_name_1) + 1;
                list_size_1 += Metadata.getLength(table1, attribute_name_1);
            }
            int blocking_factor_1 = BLOCK_SIZE / table1_record_size;

            // 두 번째 테이블의 blocking factor
            for (Map.Entry<String, Integer> entry : positionMap2.entrySet()) {
                String attribute_name_2 = entry.getKey();
                table2_record_size += Metadata.getLength(table2, attribute_name_2) + 1;
                list_size_2 += Metadata.getLength(table2, attribute_name_2);
            }
            int blocking_factor_2 = BLOCK_SIZE / table2_record_size;

            Map<Integer, List<String[]>> hashTable = new HashMap<>();

            int bytes_read;
            byte[] buffer1 = new byte[blocking_factor_1 * table1_record_size];
            byte[] record1_buffer = new byte[table1_record_size];
            int block_idx = 0;

            try (RandomAccessFile file1 = new RandomAccessFile(table1 + ".txt", "r")) {
                file1.seek(list_size_1);
                // 버퍼 크기만큼 레코드 읽어옴
                while ((bytes_read = file1.read(buffer1)) != -1) {
                    for (int i = 0; i < bytes_read / table1_record_size; i++) {
                        System.arraycopy(buffer1, i * table1_record_size, record1_buffer, 0, table1_record_size);

                        // 레코드를 '|' 구분자로 나누어 배열에 저장됨
                        String record = new String(record1_buffer, 0, table1_record_size).trim();

                        String[] tuple = record.split("\\|");
                        int keyHash = tuple[joinKeyPos1].trim().hashCode();
                        hashTable.putIfAbsent(keyHash, new ArrayList<>());
                        hashTable.get(keyHash).add(tuple);
                    }
                    block_idx++;
                    // 버퍼가 가득 찼을 때, 가장 최근에 사용한 블록의 인덱스를 갱신
                    if (block_idx % (BUFFER_SIZE / BLOCK_SIZE) == 0) {
                        lastUsedBlockIndex = (block_idx / (BUFFER_SIZE / BLOCK_SIZE)) - 1;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            byte[] buffer2 = new byte[blocking_factor_2 * table2_record_size];
            byte[] record2_buffer = new byte[table2_record_size];

            // 두 번째 테이블에서 해시 테이블을 이용하여 조인 수행
            boolean matchFound = false;

            try (RandomAccessFile file2 = new RandomAccessFile(table2 + ".txt", "r")) {
                file2.seek(list_size_2);
                // 버퍼 크기만큼 레코드 읽어옴
                while ((bytes_read = file2.read(buffer2)) != -1) {
                    for (int i = 0; i < bytes_read / table2_record_size; i++) {
                        System.arraycopy(buffer2, i * table2_record_size, record2_buffer, 0, table2_record_size);

                        // 레코드를 '|' 구분자로 나누어 배열에 저장됨
                        String record = new String(record2_buffer, 0, table2_record_size).trim();
                        String[] tuple = record.split("\\|");
                        int keyHash = tuple[joinKeyPos2].trim().hashCode();
                        if (hashTable.containsKey(keyHash)) {
                            matchFound = true;
                            for (String[] matchedTuple : hashTable.get(keyHash)) {
                                // 겹치는 속성을 제외하고 조인 결과 출력
                                StringBuilder result = new StringBuilder();
                                for (int j = 0; j < matchedTuple.length; j++) {
                                    if (j != joinKeyPos1) {
                                        result.append(matchedTuple[j]).append("|");
                                    }
                                }
                                for (int k = 0; k < tuple.length; k++) {
                                    result.append(tuple[k]);
                                    if (k != tuple.length - 1) {
                                        result.append("|");
                                    }
                                }
                                // 조인 결과 출력
                                System.out.println("조인 결과: " + result.toString());
                            }
                        }
                        hashTable.putIfAbsent(keyHash, new ArrayList<>());
                        hashTable.get(keyHash).add(tuple);
                    }
                    block_idx++;
                    // 버퍼가 가득 찼을 때, 가장 최근에 사용한 블록의 인덱스를 갱신
                    if (block_idx % (BUFFER_SIZE / BLOCK_SIZE) == 0) {
                        lastUsedBlockIndex = (block_idx / (BUFFER_SIZE / BLOCK_SIZE)) - 1;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            // 일치하는 항목이 없는 경우 첫 번째 테이블의 내용만 출력
            if (!matchFound) {
                try (RandomAccessFile file1 = new RandomAccessFile(table1 + ".txt", "r")) {
                    file1.seek(list_size_1);
                    // 버퍼 크기만큼 레코드 읽어옴
                    while ((bytes_read = file1.read(buffer1)) != -1) {
                        for (int i = 0; i < bytes_read / table1_record_size; i++) {
                            System.arraycopy(buffer1, i * table1_record_size, record1_buffer, 0, table1_record_size);

                            // 레코드를 '|' 구분자로 나누어 배열에 저장됨
                            String record = new String(record1_buffer, 0, table1_record_size).trim();
                        }
                        block_idx++;
                        // 버퍼가 가득 찼을 때, 가장 최근에 사용한 블록의 인덱스를 갱신
                        if (block_idx % (BUFFER_SIZE / BLOCK_SIZE) == 0) {
                            lastUsedBlockIndex = (block_idx / (BUFFER_SIZE / BLOCK_SIZE)) - 1;
                        }
                    }
                    String line;
                    file1.readLine(); // 첫 번째 줄을 건너뛰기
                    while ((line = file1.readLine()) != null) {
                        // 첫 번째 테이블의 튜플을 그대로 출력
                        System.out.println("조인 결과: " + line);
                    }
                } catch (IOException e) {
                    System.out.println("첫 번째 테이블을 읽는 동안 오류가 발생했습니다.");
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("해당 테이블 중 하나 이상이 존재하지 않습니다.");
        }
    }


    public static void createRelationTable() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("관계 테이블의 이름과 속성 개수, Primary Key 개수를 입력하세요.");
        System.out.print("이름, 속성 개수, Primary Key 개수 (콤마로 구분): ");
        String relation_input = scanner.nextLine();

        // 입력값 파싱
        String[] table_data = relation_input.split(",");
        String table_name = table_data[0].trim();
        attribute_count = Integer.parseInt(table_data[1].trim());
        primary_key_count = Integer.parseInt(table_data[2].trim());

        FILE_PATH = (table_name + ".txt");
        Metadata.saveRelationMetadata(table_name, attribute_count, FILE_PATH);

        // 각 속성에 대한 정보 입력 받기
            for (int i = 0; i < attribute_count; i++) {
                System.out.print("속성 이름, 데이터 타입, 길이 (콤마로 구분): ");
                String[] attribute_input = scanner.nextLine().split(",");
                String attribute_name = attribute_input[0].trim();
                String domain_type = attribute_input[1].trim();
                int position = i;
                int length = Integer.parseInt(attribute_input[2].trim());
                RECORD_SIZE += length;

                Metadata.saveAttributeMetadata(table_name, attribute_name, domain_type, position, length);
            }
            BLOCKING_FACTOR = BLOCK_SIZE / RECORD_SIZE;
            scanner.close();

            // 생성한 파일의 첫 부분에 freeList 초기화
            try (RandomAccessFile file = new RandomAccessFile(FILE_PATH, "rw")) {
                // RECORD 크기만큼 freeList 할당
                for (int i = 0; i < RECORD_SIZE; i++) {
                    // - 으로 빈 위치 나타냄
                    file.write('-');
                }
                file.write('\n');
            } catch (IOException e) {
                System.out.println("Free List 초기화에 실패했습니다.");
                e.printStackTrace();
            }

        // 관계 테이블 생성 완료 메시지 출력
        System.out.println("관계 테이블 " + table_name + "이(가) 생성되었습니다.");
    }

    public static void tupleInsert() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("데이터를 입력할 테이블 이름을 입력해주십시오.");
        String relation_name = scanner.nextLine();
        boolean table_exists = Metadata.searchRelationMetadata(relation_name);
        if (table_exists) {
            FILE_PATH = relation_name + ".txt";
            System.out.println("튜플에 들어갈 데이터를 ,구분자로 입력해주십시오.");
            String tuple_input = scanner.nextLine();
            String[] tuple = tuple_input.split(",");

            // 속성의 위치를 메타데이터에서 가져온다
            Map<String, Integer> position_map = Metadata.getPosition(relation_name);

            String[] adjustedTuple = new String[position_map.size()];

            // 속성 이름과 위치를 받아와서 속성 값과 속성 위치를 일치시키고 해당하는 길이만큼 padding값 넣어 줌
            for (Map.Entry<String, Integer> entry : position_map.entrySet()) {
                String attribute_name = entry.getKey();
                int position = entry.getValue();

                String attribute_value = tuple[position];

                int attribute_length = Metadata.getLength(relation_name, attribute_name);

                String adjusted_value = adjustValue(attribute_value, attribute_length);

                adjustedTuple[position] = adjusted_value;
            }

            if (FreeList.hasFreePosition()) {
                // freePositions 초기화
                // FreeList.loadFreeListFromFile(FILE_PATH, BUFFER_SIZE);
                // 프리 리스트에 빈 위치가 있는 경우 해당 위치에 삽입
                int freePosition = FreeList.getFreePosition(FILE_PATH);
                System.out.println("레코드 삽입 위치: " + freePosition);
                try (RandomAccessFile writer = new RandomAccessFile(FILE_PATH, "rw")) {
                    writer.seek(freePosition);
                    String record = String.join("|", adjustedTuple) + "\n";
                    writer.write(record.getBytes(StandardCharsets.UTF_8));
                    System.out.println("튜플이 성공적으로 삽입되었습니다.");
                    RECORD_CNT += 1;
                } catch (IOException e) {
                    System.out.println("데이터를 파일에 쓸 수 없습니다.");
                    e.printStackTrace();
                }
            } else {
                // 프리 리스트에 빈 위치가 없는 경우 파일의 끝에 삽입
                try (FileWriter writer = new FileWriter(FILE_PATH, true)) {
                    writer.write(String.join("|", adjustedTuple) + "\n");
                    System.out.println("튜플이 성공적으로 삽입되었습니다.");
                    RECORD_CNT += 1;
                } catch (IOException e) {
                    System.out.println("데이터를 파일에 쓸 수 없습니다.");
                    e.printStackTrace();
                }
            }
//            try (FileWriter writer = new FileWriter(FILE_PATH, true)) {
//                writer.write(String.join("|",adjustedTuple) +"\n");
//                System.out.println("튜플이 성공적으로 삽입되었습니다.");
//                RECORD_CNT += 1;
//            } catch (IOException e) {
//                System.out.println("데이터를 파일에 쓸 수 없습니다.");
//                e.printStackTrace();
//            }
        } else {
            System.out.println("해당 테이블이 존재하지 않습니다.");
        }
    }

    private static String adjustValue(String value, int length) {
        if (value.length() < length) {
            // 입력된 값의 길이가 속성의 길이보다 짧을 경우 공백을 추가하여 길이를 맞춥니다.
            StringBuilder paddedValue = new StringBuilder(value);
            for (int i = value.length(); i < length; i++) {
                paddedValue.append(" ");
            }
            return paddedValue.toString();
        } else if (value.length() > length) {
            // 입력된 값의 길이가 속성의 길이보다 길 경우, 속성의 길이만큼 값을 잘라냅니다.
            return value.substring(0, length);
        } else {
            // 입력된 값의 길이가 속성의 길이와 같으면 그대로 반환합니다.
            return value;
        }
    }

    public static int tupleSearch(String relation_name,  int primary_key_count, String[] primary_keys) {
        int searchRecordPosition = -1; // 레코드 저장 변수
        int record_size = 0;
        int list_size = 0;

        Map<String, Integer> positionMap = Metadata.getPosition(relation_name);

        for (Map.Entry<String, Integer> entry : positionMap.entrySet()) {
            String attribute_name = entry.getKey();
            record_size += Metadata.getLength(relation_name, attribute_name) + 1;
        }
        for (Map.Entry<String, Integer> entry : positionMap.entrySet()) {
            String attribute_name = entry.getKey();
            list_size += Metadata.getLength(relation_name, attribute_name);
        }

        BLOCKING_FACTOR = BLOCK_SIZE / record_size;

        try (RandomAccessFile file = new RandomAccessFile(FILE_PATH, "r")) {
            file.seek(list_size);
            byte[] buffer = new byte[BLOCKING_FACTOR * record_size];
            byte[] record_buffer = new byte[record_size];
            int bytes_read;
            int block_idx = 0;

            // 버퍼 크기만큼 레코드 읽어옴
            while ((bytes_read = file.read(buffer)) != -1) {
                for (int i = 0; i < bytes_read / record_size; i++) {
                    System.arraycopy(buffer, i * record_size, record_buffer, 0, record_size);

                    // 레코드를 '|' 구분자로 나누어 배열에 저장됨
                    String record = new String(record_buffer, 0, record_size).trim();
                    // 확인용 출력
                    System.out.println(record);
                    String[] fields = record.split("\\|");

                    // primary key 비교하여 해당하는 값 출력
                    boolean found = true;
                    for (int j = 0; j < primary_key_count; j++) {
                        if (!fields[j].trim().equals(primary_keys[j])) {
                            System.out.println(fields[j]);
                            System.out.println(primary_keys[j]);
                            found = false;
                            break;
                        }
                    }
                    if (found) {
                        // enter
                        searchRecordPosition = (block_idx + i) * (record_size * BLOCKING_FACTOR);
                        return searchRecordPosition;
                    }
                }
                block_idx++;
                // 버퍼가 가득 찼을 때, 가장 최근에 사용한 블록의 인덱스를 갱신
                if (block_idx % (BUFFER_SIZE / BLOCK_SIZE) == 0) {
                    lastUsedBlockIndex = (block_idx / (BUFFER_SIZE / BLOCK_SIZE)) - 1;
                }
            }
        } catch (IOException e) {
            System.out.println("데이터 파일에 접근할 수 없습니다.");
            e.printStackTrace();
        }
        return searchRecordPosition;
    }

    public static void tupleDelete() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("데이터를 삭제할 테이블을 입력해주십시오.");
        String relation_name = scanner.nextLine();
        boolean table_exists = Metadata.searchRelationMetadata(relation_name);

        int record_size = 0;

        Map<String, Integer> positionMap = Metadata.getPosition(relation_name);
        for (Map.Entry<String, Integer> entry : positionMap.entrySet()) {
            String attribute_name = entry.getKey();
            record_size += Metadata.getLength(relation_name, attribute_name) + 1;
        }

        if (table_exists) {
            FILE_PATH = relation_name+".txt";
            System.out.println("삭제할 튜플의 primary key 개수를 입력해주십시오.");
            int primary_key_count = Integer.parseInt(scanner.nextLine());

            System.out.println("삭제할 튜플의 primary key를 순서대로 ,구분자로 입력해주십시오.");
            String delete_input = scanner.nextLine();
            String[] delete_tuple = delete_input.split(",");

            if (delete_tuple.length != primary_key_count) {
                System.out.println("잘못된 입력 다시 입력해주세요");
                return;
            }

            int deleteRecordPosition = tupleSearch(relation_name, primary_key_count, delete_tuple);
            System.out.println(deleteRecordPosition);
            if(deleteRecordPosition != -1) {
                // 삭제된 레코드 위치 free list에 추가
                FreeList.addFreePosition(deleteRecordPosition, FILE_PATH);

                try (RandomAccessFile file = new RandomAccessFile(FILE_PATH, "rw")) {
                    // 삭제할 레코드의 위치로 이동
                    file.seek(deleteRecordPosition);
                    // 삭제할 레코드의 크기만큼 빈칸으로 채워 넣음 enter 제외
                    for (int i = 1; i < record_size; i++) {
                        // - 으로 빈 위치 나타냄
                        file.write('-');
                    }

                    System.out.println("삭제되었습니다.");

                } catch (IOException e) {
                    System.out.println("데이터 파일에 접근할 수 없습니다.");
                    e.printStackTrace();
                }
            } else {
                System.out.println("삭제할 튜플을 찾을 수 없습니다.");
            }
        }
    }


    public static void tupleQuery() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("출력할 테이블을 입력하세요.");
        String relation_name = scanner.nextLine();
        boolean table_exists = Metadata.searchRelationMetadata(relation_name);

        if (table_exists) {
            FILE_PATH = relation_name+".txt";
            int record_size = 0;
            int list_size = 0;

            Map<String, Integer> positionMap = Metadata.getPosition(relation_name);

            for (Map.Entry<String, Integer> entry : positionMap.entrySet()) {
                String attribute_name = entry.getKey();
                // metadata를 통해 record size 계산 (구분자, enter 함께 계산: 75bytes)
                record_size += Metadata.getLength(relation_name, attribute_name) + 1;
            }

            for (Map.Entry<String, Integer> entry : positionMap.entrySet()) {
                String attribute_name = entry.getKey();
                // metadata를 통해 free list size 계산
                list_size += Metadata.getLength(relation_name, attribute_name);
            }


            BLOCKING_FACTOR = BLOCK_SIZE / record_size;

            System.out.println("(1) 전체 출력\n(2) 튜플 검색\n번호를 입력하세요.");
            int num;
            try {
                num = Integer.parseInt(scanner.nextLine()); // 개행 문자 처리를 위해 nextLine() 사용 후 변환

                if (num == 1) {
                    System.out.println("모든 튜플을 출력합니다.");
                    try (RandomAccessFile file = new RandomAccessFile(FILE_PATH, "r")) {
                        // free list 건너 뛰고 출력
                        file.seek(list_size);
                        int blockIndex = 0;

                        while (true) {
                            // blocking factor만큼 읽어온다.
                            byte[] blockBuffer = new byte[BLOCKING_FACTOR * record_size]; // 블록 버퍼 초기화
                            int bytesRead = file.read(blockBuffer);
                            if (bytesRead == -1) break; // 파일 끝에 도달하면 종료

                            System.out.println("Block " + blockIndex + ":");

                            // 블록에서 레코드를 읽어와 출력
                            for (int i = 0; i < bytesRead / record_size; ++i) {
                                int recordStartIndex = i * record_size;
                                byte[] recordBuffer = new byte[record_size];
                                System.arraycopy(blockBuffer, recordStartIndex, recordBuffer, 0, record_size);
                                String record = new String(recordBuffer, StandardCharsets.UTF_8).trim();
                                System.out.println(record);
                            }
                            // 3개의 레코드를 출력한 후에는 다음 블록으로 이동
                            blockIndex++;
                        }
                    } catch (IOException e) {
                        System.out.println("데이터 파일에 접근할 수 없습니다.");
                        e.printStackTrace();
                    }
                } else if (num == 2) {
                    System.out.println("검색어를 입력하세요.");
                    String search = scanner.nextLine();
                    try (RandomAccessFile file = new RandomAccessFile(FILE_PATH, "r")) {
                        file.seek(list_size);
                        byte[] blockBuffer = new byte[BLOCKING_FACTOR * record_size];
                        int bytesRead;
                        int blockIndex = 0;
                        boolean recordFound = false;

                        while ((bytesRead = file.read(blockBuffer)) != -1) {
                            System.out.println("Block " + blockIndex++ + ":");

                            for (int i = 0; i < bytesRead / record_size; i++) {
                                int recordStartIndex = i * record_size;
                                byte[] recordBuffer = new byte[record_size];
                                System.arraycopy(blockBuffer, recordStartIndex, recordBuffer, 0, record_size);
                                String record = new String(recordBuffer).trim();
                                // 검색어가 레코드에 포함되어 있는지 확인
                                if (record.contains(search)) {
                                    System.out.println(record);
                                    recordFound = true;
                                }
                            }

                            if (recordFound && blockIndex == BLOCKING_FACTOR) break;

                            if (blockIndex > BLOCKING_FACTOR) {
                                System.out.println("해당하는 레코드가 없습니다.");
                                break;
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("데이터 파일에 접근할 수 없습니다.");
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("다시 입력해주세요.");
                }
            } catch (NumberFormatException e) {
                System.out.println("다시 입력해주세요.");
            }
        } else {
            System.out.println("존재하지 않는 테이블");
        }
    }
}
