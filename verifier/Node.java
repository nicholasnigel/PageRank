import java.util.*;

public class Node {
    private int id;
    private double pagerank, pagerankHolder;    // pagerankHolder; just holds the value temporarily to be swapped to the pagerank
    
    private List<Node> outgoingNodes, incomingNodes;

    public Node(){
        outgoingNodes = new ArrayList<Node>();
        incomingNodes = new ArrayList<Node>();
    }
    public Node(int id) {
        this();
        this.id = id;
    }
    public Node(int id, double pagerank) {
        this(id); 
        this.pagerank = pagerank;
    }

    public int getId(){
        return id;
    }
    public double getPageRank(){
        return pagerank;
    }
    public List<Node> getOutGoingNodes(){
        return outgoingNodes;
    }
    public List<Node> getIncomingNodes(){
        return incomingNodes;
    }
    public double getPageRankHolder(){
        return pagerankHolder;
    }
    public void setID(int id) {
        this.id = id;
    }
    public void setPageRank(double pagerank) {
        this.pagerank = pagerank;
    }
    public void addPageRank(double add){
        this.pagerank += add;
    }
    public void addPageRankHolder(double add) {
        this.pagerankHolder += add;
    }
    public void addToOutgoing(Node n) {
        outgoingNodes.add(n);
    }
    public void addToIncoming(Node n) {
        incomingNodes.add(n);
    }
    public void setPageRankHolder(double p) {
        this.pagerankHolder = p;
    }
    public int outgoingSize(){
        return outgoingNodes.size();
    }
    public int incomingSize(){
        return incomingNodes.size();
    }
    /** first phase of PageRank (distributing value to all adjacent nodes*/
    public double sendSplits(){
        if(outgoingNodes.size() == 0 ) return pagerank;
        double split = pagerank/outgoingNodes.size();
        for(Node out: outgoingNodes) {
            out.addPageRankHolder(split);
        }
        return 0.0; // 
    }
    /**getting distribution of mass/N and add it into pagerank value*/
    public void getDistrbutedMass(int N, double mass) {
        this.addPageRankHolder(mass/N);
    }

    /**calculate the final PageRank value*/
    public void calculatePageRank(int N, double alpha) {
        // p' = alpha ( 1/N) + (1-alpha)(p+m/n)
        double temp = getPageRankHolder();
        double replacementPR = alpha* (1.0/N) + ((1 - alpha) * temp);
        setPageRankHolder(replacementPR);
    }
    
    /**Reset the pagerank holder to 0, while replacing the current pagerank to the temporary one] */
    public void replacePageRank() {
        setPageRank(getPageRankHolder());
        setPageRankHolder(0.0);     // reset the temporary 
        
    }

}