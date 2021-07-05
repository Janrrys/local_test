import java.util.Scanner;

public class AIMind {
    public static void main(String[] args) {
        //价值10亿的AI核心代码
        Scanner sc = new Scanner(System.in);
        while (true){
            String str = sc.next();
            str = str.replaceAll("吗","");
            str = str.replaceAll("？","!");
            //str = str.replaceAll("你","我");
            System.out.println(str);
        }
    }
}
