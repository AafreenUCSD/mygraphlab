 #include <string>
  #include <graphlab.hpp>

  struct web_page {
    std::string pagename;
    double pagerank;
    web_page():pagerank(0.0) { }
    explicit web_page(std::string name):pagename(name),pagerank(0.0){ }

    void save(graphlab::oarchive& oarc) const {
      oarc << pagename << pagerank;
    }

    void load(graphlab::iarchive& iarc) {
      iarc >> pagename >> pagerank;
    }
  };
 
  typedef graphlab::distributed_graph<web_page, graphlab::empty> graph_type;


  bool line_parser(graph_type& graph, 
                   const std::string& filename, 
                   const std::string& textline) {
    std::stringstream strm(textline);
    graphlab::vertex_id_type vid;
    std::string pagename;
    // first entry in the line is a vertex ID
    strm >> vid;
    strm >> pagename;
    // insert this web page
    graph.add_vertex(vid, web_page(pagename));
    // while there are elements in the line, continue to read until we fail
    while(1){
      graphlab::vertex_id_type other_vid;
      strm >> other_vid;
      if (strm.fail())
        return true;
      graph.add_edge(vid, other_vid);
    }
    return true;
  }

class pagerank_program :
            public graphlab::ivertex_program<graph_type, double>,
            public graphlab::IS_POD_TYPE {
public:
  // we are going to gather on all the in-edges
  edge_dir_type gather_edges(icontext_type& context,
                             const vertex_type& vertex) const {
    return graphlab::IN_EDGES;
  }

  // for each in-edge gather the weighted sum of the edge.
  double gather(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
    return edge.source().data().pagerank / edge.source().num_out_edges();
  }
  
  // Use the total rank of adjacent pages to update this page 
  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
    double newval = total * 0.85 + 0.15;
    vertex.data().pagerank = newval;
  }
  
  // No scatter needed. Return NO_EDGES 
  edge_dir_type scatter_edges(icontext_type& context,
                              const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; 

class graph_writer {
   public:
      std::string save_vertex(graph_type::vertex_type v) {
        std::stringstream strm;
        // remember the \n at the end! This will provide a line break
        // after each page.
        strm << v.data().pagename << "\t" << v.data().pagerank << "\n";
        return strm.str();
      }
      std::string save_edge(graph_type::edge_type e) { return ""; }
 };



  int main(int argc, char** argv) {
    graphlab::mpi_tools::init(argc, argv);
    graphlab::distributed_control dc;
 
    graph_type graph(dc);
    graph.load("graph.txt", line_parser);

    graphlab::mpi_tools::finalize();
    graphlab::omni_engine<pagerank_program> engine(dc, graph, "sync");
    engine.signal_all();
    engine.start();
    graph.save("output",
            graph_writer(),
            false, // set to true if each output file is to be gzipped
            true, // whether vertices are saved
            false); // whether edges are saved
  }
