import java.util.*;
public class Graph {
    private List<Node> nodeList;
    
    public Graph() {
        nodeList = new ArrayList<Node>();
    }
    
    public void appendNode(Node n) {
        nodeList.add(n);
    }
    /**getting the number of nodes in the graph */
    public int getSize(){
        return nodeList.size();
    }
    public void addNode(int id) {
        Node x = new Node(id);
        appendNode(x);
    }
    public void addLink(int id1, int id2){
        Node x = find(id1);
        if(x == null) {
            x = new Node(id1);
            appendNode(x);
        }
        Node y = find(id2);
        if(y == null) {
            y = new Node(id2);
            appendNode(y);
        }
        x.addToOutgoing(y);
        y.addToIncoming(x);
    }
    public Node find(int id) {
        for(Node x: nodeList) {
            if(x.getId() == id) return x;
        }
        return null;
    }

    public List<Node> getList(){
        return nodeList;
    }
    
    // to initialize pagerank value
    public void initializePageRank() {
        int length = nodeList.size();
        for(Node n: nodeList ){
            n.setPageRank(1.0/length);
        }
    }
    
    public void iteratePageRank(double alpha) {
        // 1 iteration: 2 phases, first send data to all, second consider random jump and mass loss distribution
        // all should store in pagerankHolder
        double massloss = 0;
        for(Node n: nodeList) {
            massloss += n.sendSplits(); // get the mass loss value
        }
        System.out.println("massloss: "+massloss);
        for(Node n: nodeList){
            System.out.println(n.getId()+ " "+n.getPageRankHolder());
        }
        System.out.println("---------");
        // pageRankHolder updated already here

        for(Node n: nodeList) {
            n.getDistrbutedMass(getSize(), massloss);
            System.out.println(n.getId()+" "+n.getPageRankHolder());
            n.calculatePageRank(getSize(), alpha);
            n.replacePageRank();
        }        
        
    }
    
    /**do the pagerank algorithm for n iterations given a certain random jump parameter */
    public void pagerank(int iterations, double alpha) {
        initializePageRank();
        for(; iterations >= 1; iterations --) {
            iteratePageRank(alpha);
        }
    }  

    public void getPageRankResult() {
        System.out.println("-----------Result-----------");
        for(Node n: nodeList) {
            System.out.println(n.getId()+ " " + n.getPageRank());
        }
    }

    public static void main(String[] args) throws Exception {
        Graph G = new Graph();
        G.addLink(4, 1);        
        G.addLink(1, 2);
        G.addLink(1, 3);
        G.addLink(2, 3);
        G.addLink(3, 1);
        G.addLink(5, 6);
        G.pagerank(2, 0.2);
        
        G.getPageRankResult();
    }
}   
