package interview;

public class P01 {

    /**
     * 单链表类
     */
    class LinkNode {
        public String value;
        public LinkNode next;

        public LinkNode(String v, LinkNode next) {
            this.value = v;
            this.next = next;
        }

        public String toString() {
            return "value:" + value + ",next:" + (this.next == null ? "null" : this.next.value);
        }
    }

    /**
     * 生成测试数据
     *
     * @return
     */
    LinkNode buildTestData() {
        LinkNode n3_2 = new LinkNode("3", null);
        LinkNode n3_1 = new LinkNode("3", n3_2);
        LinkNode n2_2 = new LinkNode("2", n3_1);
        LinkNode n2_1 = new LinkNode("2", n2_2);
        LinkNode n1_2 = new LinkNode("1", n2_1);
        LinkNode n1_1 = new LinkNode("1", n1_2);
        return n1_1;
    }

    /**
     * 辅助打印输出
     *
     * @param head
     */
    void print(LinkNode head) {
        if (head == null) {
            System.out.println("null");
            return;
        }
        while (head.next != null) {
            System.out.print(head.value + "->");
            head = head.next;
        }
        System.out.print(head.value + "\n");
    }

    /**
     * 删除有序单链表中的重复元素
     *
     * @param node
     * @return
     */
    LinkNode removeDuplicate(LinkNode node) {
        if (node == null || node.next == null) {
            return node;
        }
        LinkNode dummy = new LinkNode("", node);
        LinkNode slow = dummy.next;
        LinkNode fast = slow.next;
        while (true) {
            if (!fast.value.equalsIgnoreCase(slow.value)) {
                //从链接中摘掉重复节点
                slow.next = fast;
                //慢指针前移
                slow = slow.next;
            }
            //快指针前移
            fast = fast.next;
            if (fast == null) {
                slow.next = null;
                break;
            }
        }
        return dummy.next;
    }


    public static void main(String[] args) {
        P01 p = new P01();
        LinkNode testNode = p.buildTestData();
        p.print(testNode);

        //删除有序单链表中的重复元素
        p.print(p.removeDuplicate(testNode));
    }
}
