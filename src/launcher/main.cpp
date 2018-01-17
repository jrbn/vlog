//VLog
#include <vlog/reasoner.h>
#include <vlog/materialization.h>
#include <vlog/seminaiver.h>
#include <vlog/edbconf.h>
#include <vlog/edb.h>
#include <vlog/webinterface.h>
#include <vlog/fcinttable.h>
#include <vlog/exporter.h>
#include <vlog/LogisticRegression.h>
//Used to load a Trident KB
#include <launcher/vloglayer.h>
#include <trident/loader.h>
#include <kognac/utils.h>

//RDF3X
#include <cts/parser/SPARQLLexer.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <cts/semana/SemanticAnalysis.hpp>
#include <cts/plangen/PlanGen.hpp>
#include <cts/codegen/CodeGen.hpp>
#include <rts/runtime/Runtime.hpp>
#include <rts/runtime/QueryDict.hpp>
#include <rts/operator/Operator.hpp>
#include <rts/operator/PlanPrinter.hpp>
#include <rts/operator/ResultsPrinter.hpp>

//Boost
#include <boost/chrono.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>
// #include <boost/sort/spreadsort/integer_sort.hpp>

//TBB
// Don't use global_control, to allow for older TBB versions.
// #define TBB_PREVIEW_GLOBAL_CONTROL 1
#include <tbb/task_scheduler_init.h>
// #include <tbb/global_control.h>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <chrono>
#include <thread>
#include <csignal>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace std;
namespace timens = boost::chrono;
namespace logging = boost::log;
namespace fs = boost::filesystem;
namespace po = boost::program_options;

void initLogging(logging::trivial::severity_level level) {
    logging::add_common_attributes();
    logging::add_console_log(std::cerr,
            logging::keywords::format =
            (logging::expressions::stream << "["
             << logging::expressions::attr <
             boost::log::attributes::current_thread_id::value_type > (
                 "ThreadID") << " "
             << logging::expressions::format_date_time <
             boost::posix_time::ptime > ("TimeStamp",
                 "%H:%M:%S") << " - "
             << logging::trivial::severity << "] "
             << logging::expressions::smessage));
    boost::shared_ptr<logging::core> core = logging::core::get();
    core->set_filter(logging::trivial::severity >= level);
}

void printHelp(const char *programName, po::options_description &desc) {
    cout << "Usage: " << programName << " <command> [options]" << endl << endl;
    cout << "Possible commands:" << endl;
    cout << "help\t\t produce help message." << endl;
    cout << "mat\t\t perform a full materialization." << endl;
    cout << "query\t\t execute a SPARQL query." << endl;
    cout << "queryLiteral\t\t execute a Literal query." << endl;
    cout << "server\t\t starts in server mode." << endl;
    cout << "load\t\t load a Trident KB." << endl;
    cout << "test\t\t test rule based learning." << endl;
    cout << "lookup\t\t lookup for values in the dictionary." << endl << endl;

    cout << desc << endl;
}

inline void printErrorMsg(const char *msg) {
    cout << endl << "*** ERROR: " << msg << "***" << endl << endl
        << "Please type the subcommand \"help\" for instructions (e.g. Vlog help)."
        << endl;
}

bool checkParams(po::variables_map &vm, int argc, const char** argv,
        po::options_description &desc) {

    string cmd;
    if (argc < 2) {
        printErrorMsg("Command is missing!");
        return false;
    } else {
        cmd = argv[1];
    }

    if (cmd != "help" && cmd != "query" && cmd != "lookup" && cmd != "load" && cmd != "queryLiteral"
            && cmd != "mat" && cmd != "rulesgraph" && cmd != "server" && cmd != "test") {
        printErrorMsg(
                (string("The command \"") + cmd + string("\" is unknown.")).c_str());
        return false;
    }

    if (cmd == "help") {
        printHelp(argv[0], desc);
        return false;
    } else {
        /*** Check specific parameters ***/
        if (cmd == "query" || cmd == "queryLiteral") {
            string queryFile = vm["query"].as<string>();
            if (cmd == "query" && (queryFile == ""  || !fs::exists(queryFile))) {
                printErrorMsg(
                        (string("The file ") + queryFile
                         + string(" doesn't exist.")).c_str());
                return false;
            }

            if (vm["rules"].as<string>().compare("") != 0) {
                string path = vm["rules"].as<string>();
                if (!fs::exists(path)) {
                    printErrorMsg((string("The rule file ") + path + string(" doe not exists")).c_str());
                    return false;
                }
            }
        } else if (cmd == "lookup") {
            if (!vm.count("text") && !vm.count("number")) {
                printErrorMsg(
                        "Neither the -t nor -n parameters are set. At least one of them must be set.");
                return false;
            }

            if (vm.count("text") && vm.count("number")) {
                printErrorMsg(
                        "Both the -t and -n parameters are set, and this is ambiguous. Please choose either one or the other.");
                return false;
            }
        } else if (cmd == "load") {
            if (!vm.count("input") and !vm.count("comprinput")) {
                printErrorMsg(
                        "The parameter -i (path to the triple files) is not set. Also --comprinput (file with the compressed triples) is not set.");
                return false;
            }

            if (vm.count("comprinput")) {
                string tripleDir = vm["comprinput"].as<string>();
                if (!fs::exists(tripleDir)) {
                    printErrorMsg(
                            (string("The file ") + tripleDir
                             + string(" does not exist.")).c_str());
                    return false;
                }
                if (!vm.count("comprdict")) {
                    printErrorMsg(
                            "The parameter -comprdict (path to the compressed dict) is not set.");
                    return false;
                }
            } else {
                string tripleDir = vm["input"].as<string>();
                if (!fs::exists(tripleDir)) {
                    printErrorMsg(
                            (string("The path ") + tripleDir
                             + string(" does not exist.")).c_str());
                    return false;
                }
            }

            if (!vm.count("output")) {
                printErrorMsg(
                        "The parameter -o (path to the kb is not set.");
                return false;
            }
            string kbdir = vm["output"].as<string>();
            if (fs::exists(kbdir)) {
                printErrorMsg(
                        (string("The path ") + kbdir
                         + string(" already exist. Please remove it or choose another path.")).c_str());
                return false;
            }
            if (vm["maxThreads"].as<int>() < 1) {
                printErrorMsg(
                        "The number of threads to use must be at least 1");
                return false;
            }

            if (vm["readThreads"].as<int>() < 1) {
                printErrorMsg(
                        "The number of threads to use to read the input must be at least 1");
                return false;
            }

        } else if (cmd == "mat" || cmd == "test") {
            string path = vm["rules"].as<string>();
            if (path != "" && !fs::exists(path)) {
                printErrorMsg((string("The rule file '") +
                            path + string("' does not exists")).c_str());
                return false;
            }
        }
    }

    return true;
}

bool initParams(int argc, const char** argv, po::variables_map &vm) {

    po::options_description query_options("Options for <query>, <queryLiteral> or <mat>");
    query_options.add_options()("query,q", po::value<string>()->default_value(""),
            "The path of the file with a query. It is REQUIRED with <query> or <queryLiteral>");
    query_options.add_options()("rules", po::value<string>()->default_value(""),
            "Activate reasoning during query answering using the rules defined at this path. It is REQUIRED in case the command is <mat>. Default is '' (disabled).");
    query_options.add_options()("reasoningThreshold", po::value<long>()->default_value(1000000),
            "This parameter sets a threshold to estimate the reasoning cost of a pattern. This cost can be broadly associated to the cardinality of the pattern. It is used to choose either TopDown or Magic evalution. Default is 1000000 (1M).");
    query_options.add_options()("reasoningAlgo", po::value<string>()->default_value(""),
            "Determines the reasoning algo (only for <queryLiteral>). Possible values are \"qsqr\", \"magic\", \"auto\".");
    query_options.add_options()("timeoutLiteral", po::value<int>()->default_value(0),
            "Timeout in seconds for <queryLiteral>.");
    query_options.add_options()("printValues", po::value<bool>()->default_value(true),
            "Print result triples of queries.");
    query_options.add_options()("estimationDepth", po::value<int>()->default_value(5),
            "Depth for cost estimation");
    query_options.add_options()("selectionStrategy", po::value<string>()->default_value(""),
            "Determines the selection strategy (only for <queryLiteral>, when \"auto\" is specified for the reasoningAlgorithm). Possible values are \"cardEst\", ... (to be extended) .");
    query_options.add_options()("matThreshold", po::value<long>()->default_value(10000000),
            "In case reasoning is activated, this parameter sets a threshold above which a full materialization is performed before we execute the query. Default is 10000000 (10M).");
    query_options.add_options()("automat",
            "Automatically premateralialize some atoms.");
    query_options.add_options()("timeoutPremat", po::value<int>()->default_value(1000000),
            "Timeout used during automatic prematerialization (in microseconds). Default is 1000000 (i.e. one second per query)");
    query_options.add_options()("premat", po::value<string>()->default_value(""),
            "Pre-materialize the atoms in the file passed as argument. Default is '' (disabled).");
    query_options.add_options()("multithreaded",
            "Run multithreaded (currently only supported for <mat>).");
    query_options.add_options()("nthreads", po::value<int>()->default_value(tbb::task_scheduler_init::default_num_threads() / 2),
            string("Set maximum number of threads to use when run in multithreaded mode. Default is " + to_string(tbb::task_scheduler_init::default_num_threads() / 2)).c_str());
    query_options.add_options()("interRuleThreads", po::value<int>()->default_value(0),
            "Set maximum number of threads to use for inter-rule parallelism. Default is 0");

    query_options.add_options()("shufflerules",
            "shuffle rules randomly instead of using heuristics (only for <mat>, and only when running multithreaded).");
    query_options.add_options()("repeatQuery,r",
            po::value<int>()->default_value(0),
            "Repeat the query <arg> times. If the argument is not specified, then the query will not be repeated.");
    query_options.add_options()("storemat_path", po::value<string>()->default_value(""),
            "Directory where to store all results of the materialization. Default is '' (disable).");
    query_options.add_options()("storemat_format", po::value<string>()->default_value("files"),
            "Format in which to dump the materialization. 'files' simply dumps the IDBs in files. 'db' creates a new RDF database. Default is 'files'.");
    query_options.add_options()("explain", po::value<bool>()->default_value(false),
            "Explain the query instead of executing it. Default is false.");
    query_options.add_options()("decompressmat", po::value<bool>()->default_value(false),
            "Decompress the results of the materialization when we write it to a file. Default is false.");

#ifdef WEBINTERFACE
    query_options.add_options()("webinterface", po::value<bool>()->default_value(false),
            "Start a web interface to monitor the execution. Default is false.");
    query_options.add_options()("port", po::value<int>()->default_value(8080), "Port to use for the web interface. Default is 8080");
#endif

    query_options.add_options()("no-filtering",
            "Disable filter optimization.");
    query_options.add_options()("no-intersect",
            "Disable intersection optimization.");
    query_options.add_options()("graphfile", po::value<string>(),
            "Path to store the rule dependency graph");

    po::options_description load_options("Options for <load>");
    load_options.add_options()("input,i", po::value<string>(),
            "Path to the files that contain the compressed triples. This parameter is REQUIRED if already compressed triples/dict are not provided.");
    load_options.add_options()("output,o", po::value<string>(),
            "Path to the KB that should be created. This parameter is REQUIRED.");
    load_options.add_options()("maxThreads",
            po::value<int>()->default_value(Utils::getNumberPhysicalCores()),
            "Sets the maximum number of threads to use during the compression. Default is the number of physical cores");
    load_options.add_options()("readThreads",
            po::value<int>()->default_value(2),
            "Sets the number of concurrent threads that reads the raw input. Default is '2'");
    load_options.add_options()("comprinput", po::value<string>(),
            "Path to a file that contains a list of compressed triples.");
    load_options.add_options()("comprdict", po::value<string>()->default_value(""),
            "Path to a file that contains the dictionary for the compressed triples.");

    po::options_description lookup_options("Options for <lookup>");
    lookup_options.add_options()("text,t", po::value<string>(),
            "Textual term to search")("number,n", po::value<long>(),
                "Numeric term to search");

    po::options_description test_options("Options for <test>");
    test_options.add_options()("maxTuples", po::value<unsigned int>()->default_value(100), "Sets the number of tuples to choose from database for an EDB predicate.");
    test_options.add_options()("depth", po::value<unsigned int>()->default_value(5), "Sets the depth for graph traversal when generating queries.");

    po::options_description cmdline_options("Parameters");
    cmdline_options.add(query_options).add(lookup_options).add(load_options).add(test_options);
    cmdline_options.add_options()("logLevel,l", po::value<logging::trivial::severity_level>(),
            "Set the log level (accepted values: trace, debug, info, warning, error, fatal). Default is info.");

    cmdline_options.add_options()("edb,e", po::value<string>()->default_value("default"),
            "Path to the edb conf file. Default is 'edb.conf' in the same directory as the exec file.");
    cmdline_options.add_options()("sleep",
            po::value<int>()->default_value(0),
            "sleep <arg> seconds before starting the run. Useful for attaching profiler.");

    po::store(
            po::command_line_parser(argc, argv).options(cmdline_options).run(),
            vm);

    return checkParams(vm, argc, argv, cmdline_options);
}

void lookup(EDBLayer &layer, po::variables_map &vm) {
    if (vm.count("text")) {
        uint64_t value;
        string textTerm = vm["text"].as<string>();
        if (!layer.getDictNumber((char*) textTerm.c_str(), textTerm.size(), value)) {
            cout << "Term " << textTerm << " not found" << endl;
        } else {
            cout << value << endl;
        }
    } else {
        uint64_t key = vm["number"].as<long>();
        char supportText[MAX_TERM_SIZE];
        if (!layer.getDictText(key, supportText)) {
            cout << "Term " << key << " not found" << endl;
        } else {
            cout << supportText << endl;
        }
    }
}

string flattenAllArgs(int argc,
        const char** argv) {
    string args = "";
    for (int i = 1; i < argc; ++i) {
        args += " " + string(argv[i]);
    }
    return args;
}

void writeRuleDependencyGraph(EDBLayer &db, string pathRules, string filegraph) {
    BOOST_LOG_TRIVIAL(info) << " Write the graph file to " << filegraph;
    Program p(db.getNTerms(), &db);
    p.readFromFile(pathRules);
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(db,
            &p, true, true, false, 1, 1, false);

    std::vector<int> nodes;
    std::vector<std::pair<int, int>> edges;
    sn->createGraphRuleDependency(nodes, edges);

    //Write down the details of the graph on a file
    ofstream fout(filegraph);
    fout << "#nodes" << endl;
    for (auto el : nodes)
        fout << to_string(el) << endl;
    fout << "#edges" << endl;
    for (auto el : edges)
        fout << el.first << "\t" << el.second << endl;
    fout.close();
}

void startServer(int argc,
        const char** argv,
        string pathExec,
        po::variables_map &vm) {
    std::unique_ptr<WebInterface> webint;
    int port = vm["port"].as<int>();
    webint = std::unique_ptr<WebInterface>(
            new WebInterface(NULL, pathExec + "/webinterface",
                flattenAllArgs(argc, argv),
                vm["edb"].as<string>()));
    webint->start("0.0.0.0", to_string(port));
    BOOST_LOG_TRIVIAL(info) << "Server is launched at 0.0.0.0:" << to_string(port);
    webint->join();
}

std::string makeGenericQuery(Program& p, PredId_t predId, uint8_t predCard) {
    std::string query = p.getPredicateName(predId);
    query += "(";
    for (int i = 0; i < predCard; ++i) {
        query += "V" + to_string(i+1);
        if (i != predCard-1) {
            query += ",";
        }
    }
    query += ")";
    return query;
}

typedef enum QueryType{
    QUERY_TYPE_MIXED = 0,
    QUERY_TYPE_GENERIC = 100,
    QUERY_TYPE_BOOLEAN = 1000
    }QueryType;

std::pair<std::string, int> makeComplexQuery(Program& p, Literal& l, vector<Substitution>& sub, EDBLayer& db) {
    std::string query = p.getPredicateName(l.getPredicate().getId());
    int card = l.getPredicate().getCardinality();
    query += "(";
    QueryType queryType;
    int countConst = 0;
    for (int i = 0; i < card; ++i) {
        std::string canV = "V" + to_string(i+1);
        uint8_t id = p.getIDVar(canV);
        bool found = false;
        for (int j = 0; j < sub.size(); ++j) {
            if (sub[j].origin == id) {
                char supportText[MAX_TERM_SIZE];
                db.getDictText(sub[j].destination.getValue(), supportText);
                query += supportText;
                found = true;
                countConst++;
            }
        }
        if (!found) {
            query += canV;
        }
        if (i != card-1) {
            query += ",";
        }
    }
    query += ")";

    if (countConst == card) {
        queryType = QUERY_TYPE_BOOLEAN;
    } else if (countConst == 0) {
        queryType = QUERY_TYPE_GENERIC;
    } else {
        queryType = QUERY_TYPE_MIXED;
    }
    return std::make_pair(query, queryType);
}

template <typename Generic>
std::vector<std::vector<Generic>> powerset(std::vector<Generic>& set) {
    std::vector<std::vector<Generic>> output;
    uint16_t setSize = set.size();
    uint16_t powersetSize = pow(2, setSize) - 1;
    for (int i = 1; i <= powersetSize; ++i) {
        std::vector<Generic> element;
        for (int j = 0; j < setSize; ++j) {
            if (i & (1<<j)) {
                element.push_back(set[j]);
            }
        }
        output.push_back(element);
    }
    return output;
}

PredId_t getMatchingIDB(EDBLayer& db, Program &p, vector<uint64_t>& tuple) {
    //Check this tuple with all rules
    PredId_t idbPredicateId = 65535;
    vector<Rule> rules = p.getAllRules();
    vector<Rule>::iterator it = rules.begin();
    vector<pair<uint8_t, uint64_t>> ruleTuple;
    for (;it != rules.end(); ++it) {
        vector<Literal> body = (*it).getBody();
        if (body.size() > 1) {
            continue;
        }
        uint8_t nConstants = body[0].getNConstants();
        Predicate temp = body[0].getPredicate();
        if (!p.isPredicateIDB(temp.getId())){
            // BOOST_LOG_TRIVIAL(info) << "rule : " << (*it).toprettystring(&p, &db);
            //BOOST_LOG_TRIVIAL(info) << "Cardinality of body predicate: " << +temp.getCardinality();
            int matched = 0;
            for (int c = 0; c < temp.getCardinality(); ++c) {
                uint8_t tempid = body[0].getTermAtPos(c).getId();
                if(tempid == 0) {
                    uint64_t tempvalue = body[0].getTermAtPos(c).getValue();
                    char supportText[MAX_TERM_SIZE];
                    db.getDictText(tempvalue, supportText);
                    if (tempvalue == tuple[c]) {
                        matched++;
                        //BOOST_LOG_TRIVIAL(info) << "id: " << +tempid << " Constant : " << supportText;
                    }
                }
            }
            if (matched == nConstants) {
                idbPredicateId = (*it).getHead().getPredicate().getId();
                return idbPredicateId;
            }
        }
    }
    return idbPredicateId;
}

std::vector<std::pair<std::string, int>> generateTrainingQueries(int argc,
        const char** argv,
        EDBLayer &db,
        Program &p,
        std::vector<uint8_t>& vt,
        po::variables_map &vm
        ) {
    std::unordered_map<string, int> allQueries;

    typedef std::pair<PredId_t, vector<Substitution>> EndpointWithEdge;
    typedef std::unordered_map<uint16_t, std::vector<EndpointWithEdge>> Graph;
    Graph graph;

    std::vector<Rule> rules = p.getAllRules();
    for (int i = 0; i < rules.size(); ++i) {
        Rule ri = rules[i];
        Predicate ph = ri.getHead().getPredicate();
        std::vector<Substitution> sigmaH;
        for (int j = 0; j < ph.getCardinality(); ++j) {
            VTerm dest = ri.getHead().getTuple().get(j);
            sigmaH.push_back(Substitution(vt[j], dest));
        }
        std::vector<Literal> body = ri.getBody();
        for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end(); ++itr) {
            Predicate pb = itr->getPredicate();
            std::vector<Substitution> sigmaB;
            for (int j = 0; j < pb.getCardinality(); ++j) {
                VTerm dest = itr->getTuple().get(j);
                sigmaB.push_back(Substitution(vt[j], dest));
            }
            // Calculate sigmaB * sigmaH
            std::vector<Substitution> edge_label = inverse_concat(sigmaB, sigmaH);
            EndpointWithEdge neighbour = std::make_pair(ph.getId(), edge_label);
            graph[pb.getId()].push_back(neighbour);
        }
    }

#if DEBUG
    // Try printing graph
    for (auto it = graph.begin(); it != graph.end(); ++it) {
        uint16_t id = it->first;
        std::cout << p.getPredicateName(id) << " : " << std::endl;
        std::vector<EndpointWithEdge> nei = it->second;
        for (int i = 0; i < nei.size(); ++i) {
            Predicate pred = p.getPredicate(nei[i].first);
            std::vector<Substitution> sub = nei[i].second;
            for (int j = 0; j < sub.size(); ++j){
                std::cout << p.getPredicateName(nei[i].first) << "{" << sub[j].origin << "->"
                    << sub[j].destination.getId() << " , " << sub[j].destination.getValue() << "}" << std::endl;
            }
        }
        std::cout << "=====" << std::endl;
    }
#endif

    // Gather all predicates
    std::vector<PredId_t> ids = p.getAllEDBPredicateIds();
    std::ofstream allPredicatesLog("allPredicatesInQueries.log");
    for (int i = 0; i < ids.size(); ++i) {
        int neighbours = graph[ids[i]].size();
        //BOOST_LOG_TRIVIAL(info) << +ids[i] << " : ";
            BOOST_LOG_TRIVIAL(info) << p.getPredicateName(ids[i]) << " is EDB : " << neighbours << "neighbours" <<  endl;
            Predicate edbPred = p.getPredicate(ids[i]);
            int card = edbPred.getCardinality();
            std::string query = makeGenericQuery(p, edbPred.getId(), edbPred.getCardinality());
            Literal literal = p.parseLiteral(query);
            int nVars = literal.getNVars();
            QSQQuery qsqQuery(literal);
            TupleTable *table = new TupleTable(nVars);
            db.query(&qsqQuery, table, NULL, NULL);
            uint64_t nRows = table->getNRows();
            Reasoner reasoner(vm["reasoningThreshold"].as<long>());
            std::vector<std::vector<uint64_t>> output;
            uint64_t maxTuples = vm["maxTuples"].as<unsigned int>();
            /**
             * RP1(A,B) :- TE(A, <studies>, B)
             * RP2(A,B) :- TE(A, <worksFor>, B)
             *
             * Tuple <jon, studies, VU> can match with RP2, which it should not
             *
             * All EDB tuples should be carefully matched with rules
             * */
            PredId_t predId = edbPred.getId();
            uint64_t rowNumber = 0;
            if (maxTuples > nRows) {
                maxTuples = nRows;
            }
            while (rowNumber < maxTuples) {
                std::vector<uint64_t> tuple;
                std::string tupleString("<");
                for (int j = 0; j < nVars; ++j) {
                    uint64_t value = table->getPosAtRow(rowNumber, j);
                    tuple.push_back(value);
                    char supportText[MAX_TERM_SIZE];
                    db.getDictText(value, supportText);
                    tupleString += supportText;
                    tupleString += ",";
                }
                tupleString += ">";
                BOOST_LOG_TRIVIAL(info) << "Tuple # " << rowNumber << " : " << tupleString;
                if (rowNumber == 922) {
                    BOOST_LOG_TRIVIAL(info) << "wait here";
                }
                PredId_t idbPredId = getMatchingIDB(db, p, tuple);
                if (65535 == idbPredId) {
                    BOOST_LOG_TRIVIAL(info) << "No rules found";
                    rowNumber++;
                    continue;
                }
                std::string predName = p.getPredicateName(idbPredId);

                BOOST_LOG_TRIVIAL(info) << tupleString << " ==> " << predName << " : " << +idbPredId;
                vector<Substitution> subs;
                for (int k = 0; k < card; ++k) {
                    subs.push_back(Substitution(vt[k], VTerm(0, tuple[k])));
                }
                // Find powerset of subs here
                std::vector<std::vector<Substitution>> options =  powerset<Substitution>(subs);
                unsigned int seed = (unsigned int) ((clock() ^ 413711) % 105503);
                srand(seed);
                for (int l = 0; l < options.size(); ++l) {
                    // options[l] is a set of substitutions
                    // Working variables
                    int depth = vm["depth"].as<unsigned int>();
                    vector<Substitution> sigma = options[l];
                    PredId_t predId = edbPred.getId();
                    int n = 1;
                    while (n != depth+1) {
                        uint32_t nNeighbours = graph[predId].size();
                        if (0 == nNeighbours) {
                            break;
                        }
                        uint32_t randomNeighbour;
                        if (1 == n) {
                            int index = 0;
                            bool found = false;
                            for (auto it = graph[predId].begin(); it != graph[predId].end(); ++it,++index) {
                                if (it->first == idbPredId) {
                                    randomNeighbour = index;
                                    found = true;
                                    break;
                                }
                            }
                            assert(found == true);
                        } else {
                            randomNeighbour = rand() % nNeighbours;
                        }
                        std::vector<Substitution>sigmaN = graph[predId][randomNeighbour].second;
                        std::vector<Substitution> result = concat(sigmaN, sigma);
                        PredId_t qId  = graph[predId][randomNeighbour].first;
                        uint8_t qCard = p.getPredicate(graph[predId][randomNeighbour].first).getCardinality();
                        std::string qQuery = makeGenericQuery(p, qId, qCard);
                        Literal qLiteral = p.parseLiteral(qQuery);
                        allPredicatesLog << p.getPredicateName(qId) << std::endl;
                        std::pair<string, int> finalQueryResult = makeComplexQuery(p, qLiteral, result, db);
                        std::string qFinalQuery = finalQueryResult.first;
                        int type = finalQueryResult.second + ((n > 4) ? 4 : n);
                        if (allQueries.find(qFinalQuery) == allQueries.end()) {
                            allQueries.insert(std::make_pair(qFinalQuery, type));
                        }

                        predId = qId;
                        sigma = result;
                        n++;
                    } // while the depth of exploration is reached
                } // for each partial substitution
                rowNumber++;
            }
    } // all EDB predicate ids
    allPredicatesLog.close();
    std::vector<std::pair<std::string,int>> queries;
    for (std::unordered_map<std::string,int>::iterator it = allQueries.begin(); it !=  allQueries.end(); ++it) {
        queries.push_back(std::make_pair(it->first, it->second));
        BOOST_LOG_TRIVIAL(info) << "Query: " << it->first << " type : " << it->second ;
    }
    return queries;
}

void launchFullMat(int argc,
        const char** argv,
        string pathExec,
        EDBLayer &db,
        po::variables_map &vm,
        std::string pathRules) {
    //Load a program with all the rules
    Program p(db.getNTerms(), &db);
    p.readFromFile(pathRules);

    //Set up the ruleset and perform the pre-materialization if necessary
    {
        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization mat;
            mat.guessLiteralsFromRules(p, db);
            mat.getAndStorePrematerialization(db, p, true,
                    vm["timeoutPremat"].as<int>());
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization mat;
            mat.loadLiteralsFromFile(p, vm["premat"].as<string>());
            mat.getAndStorePrematerialization(db, p, false, ~0l);
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        }

        int nthreads = vm["nthreads"].as<int>();
        if (vm["multithreaded"].empty()) {
            nthreads = -1;
        }
        int interRuleThreads = vm["interRuleThreads"].as<int>();
        if (vm["multithreaded"].empty()) {
            interRuleThreads = 0;
        }

        //Execute the materialization
        std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(db,
                &p, vm["no-intersect"].empty(),
                vm["no-filtering"].empty(),
                ! vm["multithreaded"].empty(),
                nthreads,
                interRuleThreads,
                ! vm["shufflerules"].empty());

#ifdef WEBINTERFACE
        //Start the web interface if requested
        std::unique_ptr<WebInterface> webint;
        if (vm["webinterface"].as<bool>()) {
            webint = std::unique_ptr<WebInterface>(
                    new WebInterface(sn, pathExec + "/webinterface",
                        flattenAllArgs(argc, argv),
                        vm["edb"].as<string>()));
            int port = vm["port"].as<int>();
            webint->start("localhost", to_string(port));
        }
#endif

        BOOST_LOG_TRIVIAL(info) << "Starting full materialization";
        timens::system_clock::time_point start = timens::system_clock::now();
        sn->run();
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime materialization = " << sec.count() * 1000 << " milliseconds";
        sn->printCountAllIDBs();

        if (vm["storemat_path"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();

            Exporter exp(sn);

            if (vm["storemat_format"].as<string>() == "files") {
                sn->storeOnFiles(vm["storemat_path"].as<string>(),
                        vm["decompressmat"].as<bool>(), 0);
            } else if (vm["storemat_format"].as<string>() == "db") {
                //I will store the details on a Trident index
                exp.generateTridentDiffIndex(vm["storemat_path"].as<string>());
            } else if (vm["storemat_format"].as<string>() == "nt") {
                exp.generateNTTriples(vm["storemat_path"].as<string>(), vm["decompressmat"].as<bool>());
            } else {
                BOOST_LOG_TRIVIAL(error) << "Option 'storemat_format' not recognized";
                throw 10;
            }

            boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
            BOOST_LOG_TRIVIAL(info) << "Time to index and store the materialization on disk = " << sec.count() << " seconds";
        }
#ifdef WEBINTERFACE
        if (webint) {
            //Sleep for max 1 second, to allow the fetching of the last statistics
            BOOST_LOG_TRIVIAL(info) << "Sleeping for one second to allow the web interface to get the last stats ...";
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            BOOST_LOG_TRIVIAL(info) << "Done.";
            webint->stop();
        }
#endif
    }
}

void execSPARQLQuery(EDBLayer &edb, po::variables_map &vm) {
    //Parse the rules and create a program
    Program p(edb.getNTerms(), &edb);
    string pathRules = vm["rules"].as<string>();
    bool printResults = vm["printValues"].as<bool>();
    if (pathRules != "") {
        p.readFromFile(pathRules);
        p.sortRulesByIDBPredicates();
    }

    //Set up the ruleset and perform the pre-materialization if necessary
    if (pathRules != "") {
        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization *mat = new Materialization();
            mat->guessLiteralsFromRules(p, edb);
            mat->getAndStorePrematerialization(edb, p, true,
                    vm["timeoutPremat"].as<int>());
            delete mat;
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization *mat = new Materialization();
            mat->loadLiteralsFromFile(p, vm["premat"].as<string>());
            mat->getAndStorePrematerialization(edb, p, false, ~0l);
            p.sortRulesByIDBPredicates();
            delete mat;
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        }
    }

    DBLayer *db = NULL;
    if (pathRules == "") {
        PredId_t p = edb.getFirstEDBPredicate();
        string typedb = edb.getTypeEDBPredicate(p);
        if (typedb == "Trident") {
            auto edbTable = edb.getEDBTable(p);
            KB *kb = ((TridentTable*)edbTable.get())->getKB();
            TridentLayer *tridentlayer = new TridentLayer(*kb);
            tridentlayer->disableBifocalSampling();
            db = tridentlayer;
        }
    }
    if (db == NULL) {
        if (pathRules == "") {
            // Use default rule
            p.readFromFile(pathRules);
            p.sortRulesByIDBPredicates();
        }
        db = new VLogLayer(edb, p, vm["reasoningThreshold"].as<long>(), "TI", "TE");
    }
    string queryFileName = vm["query"].as<string>();
    // Parse the query
    std::fstream inFile;
    inFile.open(queryFileName);//open the input file
    std::stringstream strStream;
    strStream << inFile.rdbuf();//read the file

    WebInterface::execSPARQLQuery(strStream.str(), vm["explain"].as<bool>(),
            edb.getNTerms(), *db, printResults, false, NULL, NULL,
            NULL);
    delete db;

    /*QueryDict queryDict(edb.getNTerms());
      QueryGraph queryGraph;
      bool parsingOk;

      SPARQLLexer lexer(strStream.str());
      SPARQLParser parser(lexer);
      boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
      parseQuery(parsingOk, parser, queryGraph, queryDict, db);
      if (!parsingOk) {
      boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
      BOOST_LOG_TRIVIAL(info) << "Runtime query: 0ms.";
      BOOST_LOG_TRIVIAL(info) << "Runtime total: " << duration.count() * 1000 << "ms.";
      BOOST_LOG_TRIVIAL(info) << "# rows = 0";
      return;
      }

    // Run the optimizer
    PlanGen *plangen = new PlanGen();
    Plan* plan = plangen->translate(db, queryGraph);
    // delete plangen;  Commented out, because this also deletes all plans!
    // In particular, it corrupts the current plan.
    // --Ceriel
    if (!plan) {
    cerr << "internal error plan generation failed" << endl;
    delete plangen;
    return;
    }
    bool explain = vm["explain"].as<bool>();
    if (explain)
    plan->print(0);

    // Build a physical plan
    Runtime runtime(db, NULL, &queryDict);
    Operator* operatorTree = CodeGen().translate(runtime, queryGraph, plan, false);

    // Execute it
    if (explain) {
    DebugPlanPrinter out(runtime, false);
    operatorTree->print(out);
    delete operatorTree;
    } else {
#if DEBUG
DebugPlanPrinter out(runtime, false);
operatorTree->print(out);
#endif
boost::chrono::system_clock::time_point startQ = boost::chrono::system_clock::now();
if (operatorTree->first()) {
while (operatorTree->next());
}
boost::chrono::duration<double> durationQ = boost::chrono::system_clock::now() - startQ;
boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
BOOST_LOG_TRIVIAL(info) << "Runtime query: " << durationQ.count() * 1000 << "ms.";
BOOST_LOG_TRIVIAL(info) << "Runtime total: " << duration.count() * 1000 << "ms.";
ResultsPrinter *p = (ResultsPrinter*) operatorTree;
long nElements = p->getPrintedRows();
BOOST_LOG_TRIVIAL(info) << "# rows = " << nElements;

delete plangen;
delete operatorTree;

int times = vm["repeatQuery"].as<int>();
if (times > 0) {
    // Redirect output
    ofstream file("/dev/null");
    streambuf* strm_buffer = cout.rdbuf();
    cout.rdbuf(file.rdbuf());

    for (int i = 0; i < times; i++) {
    PlanGen *plangen = new PlanGen();
    Plan* plan = plangen->translate(db, queryGraph);
    Runtime runtime(db, NULL, &queryDict);
    operatorTree = CodeGen().translate(runtime, queryGraph, plan, false);
    startQ = boost::chrono::system_clock::now();
    if (operatorTree->first()) {
        while (operatorTree->next());
    }
    durationQ += boost::chrono::system_clock::now() - startQ;
    p = (ResultsPrinter*) operatorTree;
    long n1 = p->getPrintedRows();
    if (n1 != nElements) {
        BOOST_LOG_TRIVIAL(error) << "Number of records (" << n1 << ") is not the same. This should not happen...";
    }
    delete plangen;
    delete operatorTree;
}
BOOST_LOG_TRIVIAL(info) << "Repeated query runtime = " << (durationQ.count() / times) * 1000
<< " milliseconds";
//Restore stdout
cout.rdbuf(strm_buffer);
}
}*/
}

pid_t pid;
bool timedOut;
void alarmHandler(int signalNumber) {
    if (signalNumber == SIGALRM) {
        kill(pid, SIGKILL);
        timedOut = true;
    }
}

string selectStrategy(EDBLayer &edb, Program &p, Literal &literal, Reasoner &reasoner, po::variables_map &vm) {
    string strategy = vm["selectionStrategy"].as<string>();
    int maxDepth = vm["estimationDepth"].as<int>();
    if (strategy == "" || strategy == "cardEst") {
        // Use the original cardinality estimation strategy
        ReasoningMode mode = reasoner.chooseMostEfficientAlgo(literal, edb, p, NULL, NULL, maxDepth);
        return mode == TOPDOWN ? "qsqr" : "magic";
    }
    // Add strategies here ...
    BOOST_LOG_TRIVIAL(error) << "Unrecognized selection strategy: " << strategy;
    throw 10;
}

double runAlgo(string algo, Literal &literal, EDBLayer &edb, Program &p, Reasoner &reasoner, po::variables_map &vm) {

    bool printResults = vm["printValues"].as<bool>();
    int nVars = literal.getNVars();
    bool onlyVars = nVars > 0;

    int times = vm["repeatQuery"].as<int>();
    int timeout = vm["timeoutLiteral"].as<int>();

    boost::chrono::system_clock::time_point startQ1 = boost::chrono::system_clock::now();

    TupleIterator *iter;

    if (algo == "edb") {
        iter = reasoner.getEDBIterator(literal, NULL, NULL, edb, onlyVars, NULL);
    } else if (algo == "magic") {
        iter = reasoner.getMagicIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
    } else if (algo == "qsqr") {
        BOOST_LOG_TRIVIAL(info) << "Calling qsqr top down iterator...";
        iter = reasoner.getTopDownIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
        BOOST_LOG_TRIVIAL(info) << "Returned from top down iterator...";
    } else if (algo == "mat") {
        iter = reasoner.getMaterializationIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
    } else {
        BOOST_LOG_TRIVIAL(error) << "Unrecognized reasoning algorithm: " << algo;
        throw 10;
    }

    long count = 0;
    int sz = iter->getTupleSize();
    if (nVars == 0) {
        cout << (iter->hasNext() ? "TRUE" : "FALSE") << endl;
        count = (iter->hasNext() ? 1 : 0);
    } else {
        while (iter->hasNext()) {
            iter->next();
            count++;
            if (printResults) {
                for (int i = 0; i < sz; i++) {
                    char supportText[MAX_TERM_SIZE];
                    uint64_t value = iter->getElementAt(i);
                    if (i != 0) {
                        cout << " ";
                    }
                    if (!edb.getDictText(value, supportText)) {
                        cerr << "Term " << value << " not found" << endl;
                        cout << value;
                    } else {
                        cout << supportText;
                    }
                }
                cout << endl;
            }
        }
    }
    boost::chrono::duration<double> durationQ1 = boost::chrono::system_clock::now() - startQ1;

    delete iter;

    if (times > 0) {
        // Redirect output
        boost::chrono::system_clock::time_point startQ = boost::chrono::system_clock::now();
        for (int j = 0; j < times; j++) {
            TupleIterator *iter = reasoner.getIterator(literal, NULL, NULL, edb, p, true, NULL);
            int sz = iter->getTupleSize();
            while (iter->hasNext()) {
                iter->next();
            }
        }
        boost::chrono::duration<double> durationQ = boost::chrono::system_clock::now() - startQ;

        BOOST_LOG_TRIVIAL(info) << "Algo = " << algo << ", query runtime = " << (durationQ1.count() * 1000) << " msec, #rows = " << count;
        BOOST_LOG_TRIVIAL(info) << "Algo = " << algo << ", repeated query runtime = " << (durationQ.count() / times) * 1000 << " milliseconds";
    } else {
        BOOST_LOG_TRIVIAL(info) << "Algo = " << algo << ", query runtime = " << (durationQ1.count() * 1000) << " msec, #rows = " << count;
    }
    return (durationQ1.count() * 1000);
}

void runLiteralQuery(EDBLayer &edb, Program &p, Literal &literal, Reasoner &reasoner, po::variables_map &vm) {

    string algo = vm["reasoningAlgo"].as<string>();
    int maxDepth = vm["estimationDepth"].as<int>();

    if (literal.getPredicate().getType() == EDB) {
        if (algo != "edb") {
            BOOST_LOG_TRIVIAL(info) << "Overriding strategy, setting it to edb";
            algo = "edb";
        }
    }

    if (algo == "both" || algo == "onlyMetrics") {
        Metrics m;
        reasoner.getMetrics(literal, NULL, NULL, edb, p, m, maxDepth);
        BOOST_LOG_TRIVIAL(info) << "Query: " << literal.tostring(&p, &edb) << " Vector: "
        << m.cost << ", " << m.estimate << ", " << m.countRules << ", "
        << m.countUniqueRules << ", "
        << m.countIntermediateQueries
        << ", " << m.countIDBPredicates;
        if (algo == "both") {
            double t2 = runAlgo("magic", literal, edb, p, reasoner, vm);
            BOOST_LOG_TRIVIAL(info) << "magic time = " << t2;
            double t1 = runAlgo("qsqr", literal, edb, p, reasoner, vm);
            BOOST_LOG_TRIVIAL(info) << "qsqr time = " << t1;
        }
        return;
    }

    if (algo == "auto" || algo == "") {
        algo = selectStrategy(edb, p, literal, reasoner, vm);
        BOOST_LOG_TRIVIAL(info) << "Selection strategy determined that we go for " << algo;
    }

    runAlgo(algo, literal, edb, p, reasoner, vm);
}


void execLiteralQuery(EDBLayer &edb, po::variables_map &vm) {
    //Parse the rules and create a program
    Program p(edb.getNTerms(), &edb);
    string pathRules = vm["rules"].as<string>();
    if (pathRules != "") {
        p.readFromFile(pathRules);
        p.sortRulesByIDBPredicates();
    }

    //Set up the ruleset and perform the pre-materialization if necessary
    if (pathRules != "") {
        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization *mat = new Materialization();
            mat->guessLiteralsFromRules(p, edb);
            mat->getAndStorePrematerialization(edb, p, true,
                    vm["timeoutPremat"].as<int>());
            delete mat;
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization *mat = new Materialization();
            mat->loadLiteralsFromFile(p, vm["premat"].as<string>());
            mat->getAndStorePrematerialization(edb, p, false, ~0l);
            p.sortRulesByIDBPredicates();
            delete mat;
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        }
    }

    string query;
    string queryFileName = vm["query"].as<string>();
    if (fs::exists(queryFileName)) {
        // Parse the query
        std::fstream inFile;
        inFile.open(queryFileName);//open the input file
        std::getline(inFile, query);
        inFile.close();
    } else {
        query = queryFileName;
    }
    Literal literal = p.parseLiteral(query);
    Reasoner reasoner(vm["reasoningThreshold"].as<long>());
    runLiteralQuery(edb, p, literal, reasoner, vm);
}

std::string executeCommand(const char* cmd, char * const args[], int timeoutSeconds) {
    int pipefd[2];
    pipe(pipefd);

    int ret;
    signal(SIGALRM, alarmHandler);
    timedOut = false;

    pid = fork();
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1] ,1);
        dup2(pipefd[1], 2);
        close(pipefd[1]);
        execv(cmd, args);
    } else {
        alarm(timeoutSeconds);
        BOOST_LOG_TRIVIAL(debug) << "waiting for child to die";
        ret = waitpid(pid, NULL, 0);
        alarm(0);
        if (timedOut) {
            return "TIMEDOUT";
        }
        fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
        string result = "<<<";
        char buffer[1024];
        while (( ret = read(pipefd[0], buffer, sizeof(buffer))) > 0) {
            result += buffer;
        }
        result += ">>>";
        return result;
    }
}

double parseOutput(std::string output) {
    int pos = output.find("query runtime = ");
    int posUnit = output.rfind(" msec");
    int posStart = pos + strlen("query runtime = ");
    int lengthTime = posUnit - posStart;
    string timeString = output.substr(posStart, lengthTime);
    double ret = atof(timeString.c_str());
    return ret;
}

int main(int argc, const char** argv) {

    //Init params
    po::variables_map vm;
    if (!initParams(argc, argv, vm)) {
        return EXIT_FAILURE;
    }
    fs::path full_path( fs::initial_path<fs::path>());
    //full_path = fs::system_complete(fs::path( argv[0]));

    //Init logging system
    logging::trivial::severity_level level =
        vm.count("logLevel") ?
        vm["logLevel"].as<logging::trivial::severity_level>() :
        logging::trivial::info;
    initLogging(level);

    string cmd = string(argv[1]);

    //Get the path to the EDB layer
    string edbFile = vm["edb"].as<string>();
    if (edbFile == "default") {
        //Get current directory
        fs::path execFile(argv[0]);
        fs::path dirExecFile = execFile.parent_path();
        edbFile = dirExecFile.string() + string("/edb.conf");
        if (cmd != "load" && !fs::exists(edbFile)) {
            printErrorMsg(string("I could not find the EDB conf file " + edbFile).c_str());
            return EXIT_FAILURE;
        }
    }

    //set up parallelism in the TBB library
    size_t parallelism = vm["nthreads"].as<int>();
    if (parallelism <= 1) {
        parallelism = 2;    // Otherwise tbb aborts.
        // Actual parallelism will be controlled elsewhere.
    }
    // Allow for older tbb versions: don't use global_control.
    // tbb::global_control c(tbb::global_control::max_allowed_parallelism, parallelism);
    tbb::task_scheduler_init init(parallelism);
    /*if (!vm["multithreaded"].empty()) {
      const size_t parallelism = vm["nthreads"].as<int>();
      if (parallelism > 1) {
    //tbb::global_control c(tbb::global_control::max_allowed_parallelism, parallelism);
    c.max_allowed_parallelism = parallelism;
    }
    }*/

    // For profiling:
    int seconds = vm["sleep"].as<int>();
    if (seconds > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(seconds * 1000));
    }

    timens::system_clock::time_point start = timens::system_clock::now();

    BOOST_LOG_TRIVIAL(debug) << "sizeof(EDBLayer) = " << sizeof(EDBLayer);

    if (cmd == "query" || cmd == "queryLiteral") {
        EDBConf conf(edbFile);
        EDBLayer *layer = new EDBLayer(conf, false);

        //Execute the query
        if (cmd == "query") {
            execSPARQLQuery(*layer, vm);
        } else {
            execLiteralQuery(*layer, vm);
        }
        delete layer;
    } else if (cmd == "lookup") {
        EDBConf conf(edbFile);
        EDBLayer *layer = new EDBLayer(conf, false);
        lookup(*layer, vm);
        delete layer;
    } else if (cmd == "mat") {
        EDBConf conf(edbFile);
        EDBLayer *layer = new EDBLayer(conf, ! vm["multithreaded"].empty());
        // EDBLayer layer(conf, false);
        launchFullMat(argc, argv, full_path.string(), *layer, vm,
                vm["rules"].as<string>());
        delete layer;
    } else if (cmd == "rulesgraph") {
        EDBConf conf(edbFile);
        EDBLayer *layer = new EDBLayer(conf, false);
        writeRuleDependencyGraph(*layer, vm["rules"].as<string>(),
                vm["graphfile"].as<string>());
        delete layer;
    } else if (cmd == "test") {
        EDBConf conf(edbFile);
        EDBLayer *layer = new EDBLayer(conf, false);
        Program p(layer->getNTerms(), layer);
        uint8_t vt1 = (uint8_t) p.getIDVar("V1");
        uint8_t vt2 = (uint8_t) p.getIDVar("V2");
        uint8_t vt3 = (uint8_t) p.getIDVar("V3");
        uint8_t vt4 = (uint8_t) p.getIDVar("V4");
        std::vector<uint8_t> vt;
        vt.push_back(vt1);
        vt.push_back(vt2);
        vt.push_back(vt3);
        vt.push_back(vt4);
        p.readFromFile(vm["rules"].as<string>());
        timens::system_clock::time_point start = timens::system_clock::now();
        std::vector<std::pair<std::string,int>> trainingQueries = generateTrainingQueries(argc, argv, *layer, p, vt, vm);
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now()- start;
        int nQueries = trainingQueries.size();
        BOOST_LOG_TRIVIAL(info) << nQueries << " queries generated in " << sec.count() << " seconds";

        int maxDepth = vm["estimationDepth"].as<int>();
        Reasoner reasoner(vm["reasoningThreshold"].as<long>());
        vector<Instance> dataset;
        // ../vlog queryLiteral -e edb-lubm.conf --rule /home/uji300/vlog/examples/rules/aaai2016/LUBM_LE.dlog --reasoningAlgo edb  -l info -q "TE(A,B,C)" | wc -l
        std::string rulesFile = vm["rules"].as<string>();
        std::string trainingFileName = fs::path(rulesFile).stem().string();
        trainingFileName += "-training.csv";
        std::ofstream csvFile(trainingFileName);

        std::string genericQueriesTrainingFileName = fs::path(rulesFile).stem().string();
        genericQueriesTrainingFileName += "-genTraining.csv";
        std::ofstream csvGenTraining(genericQueriesTrainingFileName);

        std::string magicQueriesFileName(trainingFileName);
        magicQueriesFileName += "-magicQueries.log";
        std::ofstream magicSetQueriesLog(magicQueriesFileName);

        std::string allQueriesFileName(trainingFileName);
        allQueriesFileName += "-allQueries.log";
        std::ofstream allQueriesLog(allQueriesFileName);
        start = timens::system_clock::now();
        for (int i = 0; i < nQueries; ++i) {
            vector<double> data;
            int label;
            int timeout = vm["timeoutLiteral"].as<int>();
            BOOST_LOG_TRIVIAL(info) << i << "/" << nQueries;
            BOOST_LOG_TRIVIAL(info) << "Query : " << trainingQueries[i].first;
            std::string query = trainingQueries[i].first;
            int queryType = trainingQueries[i].second;
            Literal literal = p.parseLiteral(query);
            Metrics m;
            reasoner.getMetrics(literal, NULL, NULL, *layer, p, m, maxDepth);

            std::string algorithm = "qsqr";
            std::string newCommand(argv[0]);
            char *const args [] = {const_cast<char*>(argv[0]), "queryLiteral", "-e", const_cast<char*>(edbFile.c_str()), "--rule", const_cast<char*>(rulesFile.c_str()), "--reasoningAlgo", const_cast<char*>(algorithm.c_str()), "-l", "info", "--printValues", "0", "-q", const_cast<char*>(query.c_str()), NULL};
            BOOST_LOG_TRIVIAL(info) << "Timeout for QSQR: " << timeout;
            std::string qsqrOut = executeCommand(argv[0], args, timeout);

            double qsqrTime = timeout*1000;
            if (qsqrOut != "TIMEDOUT") {
                qsqrTime = parseOutput(qsqrOut);
                if ((int)((qsqrTime/1000) + 1.0) < timeout) {
                    timeout = (qsqrTime/1000) + 1.0;
                }
            }
            BOOST_LOG_TRIVIAL(info) << "QSQR time = " << qsqrTime;
            algorithm = "magic";
            char * const args2 [] = {const_cast<char*>(argv[0]), "queryLiteral", "-e", const_cast<char*>(edbFile.c_str()), "--rule", const_cast<char*>(rulesFile.c_str()), "--reasoningAlgo", const_cast<char*>(algorithm.c_str()), "-l", "info", "--printValues", "0", "-q", const_cast<char*>(query.c_str()), NULL};
            BOOST_LOG_TRIVIAL(info) << "Timeout for MAGIC: " << timeout;
            std::string magicOut = executeCommand(argv[0], args2, timeout);
            double magicTime = timeout*1000;
            if (magicOut != "TIMEDOUT") {
                magicTime = parseOutput(magicOut);
            }
            BOOST_LOG_TRIVIAL(info) << "MAGIC time = " << magicTime;
            std::string winnerAlgo = "MAGIC"; // magic
            if (qsqrTime < magicTime) {
                winnerAlgo = "QSQR";
            }
            if (qsqrTime == magicTime) {
                BOOST_LOG_TRIVIAL(info) << query << " timed out for both algorithms. Skipping from the training set..." << endl;
                continue;
            }

            if (winnerAlgo == "MAGIC") {
                magicSetQueriesLog << query << ", "
                << m.cost << ", " << m.estimate << ", " << m.countRules << ", " << m.countUniqueRules << ","
                << m.countIntermediateQueries << ", " <<
                m.countIDBPredicates << ", " << winnerAlgo << " QSQR time = " << qsqrTime << " Magic time = " << magicTime << std::endl;
            }
            BOOST_LOG_TRIVIAL(info) << m.estimate << ", " << m.cost << ", " << m.countRules << ", " << m.countIntermediateQueries << ", " << m.countUniqueRules << " , " << m.countIDBPredicates << "," <<  winnerAlgo << std::endl;
            if (queryType < 101 || queryType > 104) {
                csvFile << m.cost << ", "
                << m.estimate << ", "
                << m.countRules << ", "
                << m.countUniqueRules << ","
                << m.countIntermediateQueries << ", "
                << m.countIDBPredicates << ", "
                << winnerAlgo << std::endl;
            }

            allQueriesLog << query << ", "
            << m.cost << ", " << m.estimate << ", " << m.countRules << ", " << m.countUniqueRules << ","
            << m.countIntermediateQueries << ", " <<
            m.countIDBPredicates << ", " << winnerAlgo << ",QSQR time = " << qsqrTime << ",Magic time = " << magicTime << std::endl;

            data.push_back(m.cost);
            data.push_back(m.estimate);
            data.push_back(m.countRules);
            data.push_back(m.countUniqueRules);
            data.push_back(m.countIntermediateQueries);
            label = (winnerAlgo == "QSQR") ? 1 : 0;
            Instance instance(label, data);
            dataset.push_back(instance);
        }
        if (csvFile.fail()) {
            BOOST_LOG_TRIVIAL(error) << "Error writing to the csv file";
        }
        csvFile.close();

        if (magicSetQueriesLog.fail()) {
            BOOST_LOG_TRIVIAL(error) << "Error writing to the magic set log file";
        }
        magicSetQueriesLog.close();

        if (allQueriesLog.fail()) {
            BOOST_LOG_TRIVIAL(error) << "Error writing to the queries log file";
        }
        allQueriesLog.close();

        boost::chrono::duration<double> secData = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << nQueries << " queries's training data is generated in " << secData.count() << " seconds";

        // Train with LogisticRegression object' train() method
        //start = timens::system_clock::now();
        //LogisticRegression lr(5);
        //lr.train(dataset);
        //boost::chrono::duration<double> secTraining = boost::chrono::system_clock::now() - start;
        //BOOST_LOG_TRIVIAL(info) << nQueries << " queries are trained with LR in " << secTraining.count() << " seconds";
        //vector<double> x = {2, 1, 1, 0, 1};
        //std::cout << "test classification = " << lr.classify(x)<< std::endl;
        delete layer;
    } else if (cmd == "load") {
        Loader *loader = new Loader();
        bool onlyCompress = false;
        int sampleMethod = PARSE_COUNTMIN;
        string dictMethod = DICT_HEURISTICS;
        int popArg = 1000;
        int nindices = 6;
        bool aggrIndices = false;
        int fixedStrat = StorageStrat::FIXEDSTRAT5;
        bool enableFixedStrat = true;
        bool storePlainList = false;
        double sampleRate = 0.01;
        bool sample = true;
        int ndicts = 1;
        bool canSkipTables = false;
        int thresholdSkipTable = 0;
        string popMethod = "hash";
        if (vm.count("comprinput")) {
            string comprinput = vm["comprinput"].as<string>();
            string comprdict = vm["comprdict"].as<string>();
            BOOST_LOG_TRIVIAL(info) << "Creating the KB from " << comprinput << "/" << comprdict;

            ParamsLoad p;
            p.inputformat = "rdf";
            p.onlyCompress = onlyCompress;
            p.inputCompressed = true;
            p.triplesInputDir =  vm["comprinput"].as<string>();
            p.dictDir = vm["comprdict"].as<string>();
            p.tmpDir = vm["output"].as<string>();
            p.kbDir = vm["output"].as<string>();
            p.dictMethod = dictMethod;
            p.sampleMethod = sampleMethod;
            p.sampleArg = popArg;
            p.parallelThreads = vm["maxThreads"].as<int>();
            p.maxReadingThreads = vm["readThreads"].as<int>();
            p.dictionaries = ndicts;
            p.nindices = nindices;
            p.createIndicesInBlocks = false;    // true not working???
            p.aggrIndices = aggrIndices;
            p.canSkipTables = canSkipTables;
            p.enableFixedStrat = enableFixedStrat;
            p.fixedStrat = fixedStrat;
            p.storePlainList = storePlainList;
            p.sample = sample;
            p.sampleRate = sampleRate;
            p.thresholdSkipTable = thresholdSkipTable;
            p.logPtr = NULL;
            p.remoteLocation = "";
            p.limitSpace = 0;
            p.graphTransformation = "";
            p.storeDicts = true;

            loader->load(p);

            /*loader->load("rdf", onlyCompress, true, vm["comprinput"].as<string>(),
              vm["comprdict"].as<string>() , vm["output"].as<string>(),
              vm["output"].as<string>(),
              dictMethod, sampleMethod,
              popArg,
              vm["maxThreads"].as<int>(), vm["readThreads"].as<int>(),
              ndicts, nindices,
              true,
              aggrIndices, canSkipTables, enableFixedStrat,
              fixedStrat, storePlainList,
              sample, sampleRate, thresholdSkipTable, NULL, "", 0, "");*/

        } else {
            BOOST_LOG_TRIVIAL(info) << "Creating the KB from " << vm["input"].as<string>();


            ParamsLoad p;
            p.inputformat = "rdf";
            p.onlyCompress = false;
            p.inputCompressed = false;
            p.triplesInputDir =  vm["input"].as<string>();
            p.dictDir = "";
            p.tmpDir = vm["output"].as<string>();
            p.kbDir = vm["output"].as<string>();
            p.dictMethod = dictMethod;
            p.sampleMethod = sampleMethod;
            p.sampleArg = popArg;
            p.parallelThreads = vm["maxThreads"].as<int>();
            p.maxReadingThreads = vm["readThreads"].as<int>();
            p.dictionaries = ndicts;
            p.nindices = nindices;
            p.createIndicesInBlocks = false;    // true not working???
            p.aggrIndices = aggrIndices;
            p.canSkipTables = canSkipTables;
            p.enableFixedStrat = enableFixedStrat;
            p.fixedStrat = fixedStrat;
            p.storePlainList = storePlainList;
            p.sample = sample;
            p.sampleRate = sampleRate;
            p.thresholdSkipTable = thresholdSkipTable;
            p.logPtr = NULL;
            p.remoteLocation = "";
            p.limitSpace = 0;
            p.graphTransformation = "";
            p.timeoutStats = 0;
            p.storeDicts = true;

            loader->load(p);


            /*loader->load("rdf", onlyCompress, false, vm["input"].as<string>(), "", vm["output"].as<string>(),
              vm["output"].as<string>(),
              dictMethod, sampleMethod,
              popArg,
              vm["maxThreads"].as<int>(), vm["readThreads"].as<int>(),
              ndicts, nindices,
              true,
              aggrIndices, canSkipTables, enableFixedStrat,
              fixedStrat, storePlainList,
              sample, sampleRate, thresholdSkipTable, NULL, "", 0, "");*/
        }
        delete loader;
    } else if (cmd == "server") {
        startServer(argc, argv, full_path.string(), vm);
    }
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(info) << "Runtime = " << sec.count() * 1000 << " milliseconds";

    //Print other stats
    BOOST_LOG_TRIVIAL(info) << "Max memory used: " << Utils::get_max_mem() << " MB";
    return EXIT_SUCCESS;
}
