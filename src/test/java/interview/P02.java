package interview;

public class P02 {
    static int[] a = new int[]{3, 5, 1, 2};
    static int[] b = new int[]{4, 2, 5, 9, 7};

    public static void main(String[] args) {
        //题目：2个int数组（如上，假设每个数组中元素的范围在0-10之间）
        //要求：对2个数组去重合并，并排序，得到1个新数组，即：[1,2,3,4,5,7,9]
        int[] c = new int[11];

        for (int i = 0; i < a.length; i++) {
            c[a[i]] += 1;
        }

        for (int i = 0; i < b.length; i++) {
            c[b[i]] += 1;
        }

        for (int i = 0; i < c.length; i++) {
            if (c[i] > 0) {
                System.out.print(i + "\t");
            }
        }
    }
}
