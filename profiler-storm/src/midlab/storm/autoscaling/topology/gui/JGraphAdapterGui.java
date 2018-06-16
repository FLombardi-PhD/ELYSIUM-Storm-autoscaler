package midlab.storm.autoscaling.topology.gui;

import java.awt.Color;
import java.awt.Dimension;
import java.util.Iterator;
import java.util.Set;

import javax.swing.JApplet;

import midlab.storm.autoscaling.topology.Topology;
import midlab.storm.autoscaling.topology.TopologyBuilder;

import org.jgraph.JGraph;
import org.jgrapht.ext.JGraphModelAdapter;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;

/**
 * Applet for graph visualization
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class JGraphAdapterGui extends JApplet {
    
	private static final long serialVersionUID = 1L;
	private static final Color     DEFAULT_BG_COLOR = Color.decode( "#FAFBFF" );
    private static final Dimension DEFAULT_SIZE = new Dimension( 530, 320 );


    /**
     * @see java.applet.Applet#init().
     */
    public void init() {
        
    	Topology t;
		try {
			t = TopologyBuilder.initializeTopology();
			ListenableDirectedWeightedGraph<String,DefaultWeightedEdge> gGui = t.getGraphGui();
			
	    	Set<DefaultWeightedEdge> edges = gGui.incomingEdgesOf("finalRanker");
	    	Iterator<DefaultWeightedEdge> itEdges = edges.iterator();
	    	DefaultWeightedEdge edge;
	    	while(itEdges.hasNext()){
	    		edge = itEdges.next();
	    		gGui.setEdgeWeight(edge, 2.5);
	    		System.out.println(edge+", w="+gGui.getEdgeWeight(edge));
	    	}
	    	
	        // create a visualization using JGraph, via an adapter
	    	@SuppressWarnings({ "rawtypes", "unchecked" })
			JGraphModelAdapter m_jgAdapter = new JGraphModelAdapter(gGui);
	        JGraph jgraph = new JGraph(m_jgAdapter);

	        adjustDisplaySettings(jgraph);
	        getContentPane().add(jgraph);
	        resize(DEFAULT_SIZE);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }


    private void adjustDisplaySettings(JGraph jg) {
    	jg.setPreferredSize( DEFAULT_SIZE );
        Color  c        = DEFAULT_BG_COLOR;
        String colorStr = null;

        try{
        	colorStr = getParameter( "bgcolor" );
        }
        catch( Exception e ) {}
        if( colorStr != null ) {
            c = Color.decode( colorStr );
        }
        jg.setBackground( c );
    }

}