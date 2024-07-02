import java.io.*;
import java.util.*;

// 첫 번째 행 행렬의 크기 n 1 ~ 10
// 두 번째 행은 0, 1 행렬 개행 문자(newline, \n)로 구분
// 출력 첫 번째 행은 영역의 개수
// 두번째 행은 영역의 크기를 공백으로 구분하여 오름차순 출력 -> 개행문자로 끝
// 입력 예시
//6
//0 1 1 0 0 0
//0 1 1 0 1 1
//0 0 0 0 1 1
//0 0 0 0 1 1
//1 1 0 0 1 0
//1 1 1 0 0 0

public class Main {
    private static int[] X = {-1, 0, 1, 0};
    private static int[] Y = {0, 1, 0, -1};
    private static int cnt;

    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        // 행렬 크기
        String input = br.readLine();
        int n = Integer.parseInt(input);
        int[][] array = new int[n][n];

        // 영역 넓이 저장
        ArrayList<Integer> arr = new ArrayList<Integer>();
        Main t = new Main();


        // 행렬 입력
        for (int i = 0; i < n; i++) {
            String in = br.readLine();
            String[] inp = in.split(" ");
            for (int j = 0; j < n; j++) {
                array[i][j] = Integer.parseInt(inp[j]);
            }
        }
        // 방문한 곳 체크
        boolean[][] visited = new boolean[n][n];

        for (int i = 0; i < n; i++){
            for (int j = 0; j < n; j++) {
                if(array[i][j] == 1 && !visited[i][j]){
                    cnt = 0;
                    t.dfs(i, j, visited, array);
                    arr.add(cnt);
                }
            }
        }
        Collections.sort(arr);
        System.out.println(arr.size());
        for (int size: arr) {
            System.out.print(size + " ");
        }
        System.out.println();

    }
    public void dfs(int x, int y, boolean[][] visited, int[][] map){
        visited[x][y] = true;
        cnt++;

        for (int i = 0; i < 4; i++) {
            int nextX = x + X[i];
            int nextY = y + Y[i];
            if (nextX < 0 || nextY < 0 || nextX >= visited.length || nextY >= visited.length) continue;
            if (!visited[nextX][nextY] && map[nextX][nextY] == 1) {
                dfs(nextX, nextY, visited, map);
            }
        }
    }
}
