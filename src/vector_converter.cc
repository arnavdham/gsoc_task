// --- APPROACH 1: STANDARD NESTED LISTS ---
// This uses the traditional arrow::ListBuilder. 
// It works for basic variable-length arrays but requires 
// data to be contiguous during the ingestion phase.

// #include <iostream>
// #include <vector>
// #include <string>

// #include "podio/Frame.h"
// #include "podio/ROOTReader.h"
// #include "edm4hep/EventHeaderCollection.h"

// #include <arrow/api.h>
// #include <arrow/io/api.h>
// #include <parquet/arrow/writer.h>

// void verify_status(const arrow::Status& status) {
//     if (!status.ok()) {
//         std::cerr << "Arrow Error: " << status.ToString() << std::endl;
//         exit(1);
//     }
// }

// int main(int argc, char** argv) {
//     if (argc < 3) {
//         std::cout << "Usage: ./vector_converter <input_file.root> <output_file.parquet>" << std::endl;
//         return 1;
//     }

//     std::string input_file = argv[1];
//     std::string output_file = argv[2];
//     auto reader = podio::ROOTReader();
    
//     try {
//         reader.openFile(input_file);
//     } catch (const std::exception& e) {
//         std::cerr << "Could not open file. Details: " << e.what() << std::endl;
//         return 1;
//     }

//     auto m_pool = arrow::default_memory_pool();

//     arrow::Int32Builder event_num_builder(m_pool);
    
//     //We need a builder for the floats inside the list
//     auto value_builder = std::make_shared<arrow::FloatBuilder>(m_pool);
//     //We wrap it in a ListBuilder
//     arrow::ListBuilder weights_list_builder(m_pool, value_builder);

//     size_t n_events = reader.getEntries("events");
//     std::cout << "Converting " << n_events << " events with Mock VectorMembers..." << std::endl;

//     for (size_t i = 0; i < n_events; ++i) {
//         auto frame = podio::Frame(reader.readEntry("events", i));
        
//         const auto& event_headers = frame.get<edm4hep::EventHeaderCollection>("EventHeader");

//         if (event_headers.isValid() && !event_headers.empty()) {
//             for (const auto& h : event_headers) {
//                 // Append standard member
//                 verify_status(event_num_builder.Append(h.getEventNumber()));

//                 // Tell the ListBuilder a new list starts for this row
//                 verify_status(weights_list_builder.Append());
                
//                 // Create a mock list: [1.0, 0.5, 0.25]
//                 std::vector<float> mock_weights = {1.0f, 0.5f, 0.25f};
//                 verify_status(value_builder->AppendValues(mock_weights));
//             }
//         }
//     }

//     // Finalize Arrays
//     std::shared_ptr<arrow::Array> ev_arr, w_arr;
//     verify_status(event_num_builder.Finish(&ev_arr));
//     verify_status(weights_list_builder.Finish(&w_arr));

//     // Define Schema with the List type
//     auto schema = arrow::schema({
//         arrow::field("eventNumber", arrow::int32()),
//         arrow::field("mock_weights", arrow::list(arrow::float32()))
//     });

//     auto table = arrow::Table::Make(schema, {ev_arr, w_arr});
    
//     auto result = arrow::io::FileOutputStream::Open(output_file);
//     auto outfile = result.ValueOrDie();

//     verify_status(parquet::arrow::WriteTable(*table, m_pool, outfile, 1024));
//     verify_status(outfile->Close());
    
//     std::cout << "Success! Created " << output_file << " with nested ListArrays." << std::endl;

//     return 0;
// }




// --- APPROACH 2: OPTIMIZED IN-MEMORY LISTVIEW ---
// Using arrow::ListViewBuilder to handle irregular memory slices.
// This is the superior technical approach for PODIO's internal memory
// but hit a limitation during the Parquet serialization phase.

// #include <iostream>
// #include <vector>
// #include <string>

// #include "podio/Frame.h"
// #include "podio/ROOTReader.h"
// #include "edm4hep/EventHeaderCollection.h"

// #include <arrow/api.h>
// #include <arrow/io/api.h>
// #include <parquet/arrow/writer.h>

// void verify_status(const arrow::Status& status) {
//     if (!status.ok()) {
//         std::cerr << "Arrow Error: " << status.ToString() << std::endl;
//         exit(1);
//     }
// }

// int main(int argc, char** argv) {
//     if (argc < 3) {
//         std::cout << "Usage: ./vector_converter <input_file.root> <output_file.parquet>" << std::endl;
//         return 1;
//     }

//     std::string input_file = argv[1];
//     std::string output_file = argv[2];
//     auto reader = podio::ROOTReader();
    
//     try {
//         reader.openFile(input_file);
//     } catch (const std::exception& e) {
//         std::cerr << "Could not open file. Details: " << e.what() << std::endl;
//         return 1;
//     }

//     auto m_pool = arrow::default_memory_pool();

//     // Standard builders
//     arrow::Int32Builder event_num_builder(m_pool);
    
//     // LISTVIEW BUILDERS
//     auto value_builder = std::make_shared<arrow::FloatBuilder>(m_pool);
//     // ListViewBuilder needs the value_builder to know what type it's nesting
//     arrow::ListViewBuilder weights_view_builder(m_pool, value_builder);

//     size_t n_events = reader.getEntries("events");
//     std::cout << "Converting " << n_events << " events using Variable-Size ListView" << std::endl;

//     for (size_t i = 0; i < n_events; ++i) {
//         auto frame = podio::Frame(reader.readEntry("events", i));
        
//         const auto& event_headers = frame.get<edm4hep::EventHeaderCollection>("EventHeader");

//         if (!event_headers.empty()) {
//             for (const auto& h : event_headers) {
//                 verify_status(event_num_builder.Append(h.getEventNumber()));

//                 // Calculate dynamic length: 1, 2, or 3
//                 int64_t n_weights = (i % 3) + 1; 

//                 // ListViewBuilder::Append expects (is_valid, list_length)
//                 verify_status(weights_view_builder.Append(true, n_weights));
                
//                 std::vector<float> dynamic_weights;
//                 for(int j = 0; j < n_weights; ++j) {
//                     dynamic_weights.push_back(1.0f / (j + 1)); 
//                 }
                
//                 verify_status(value_builder->AppendValues(dynamic_weights));
//             }
//         }
//     }

//     // Finalize Arrays
//     std::shared_ptr<arrow::Array> ev_arr, w_arr;
//     verify_status(event_num_builder.Finish(&ev_arr));
//     verify_status(weights_view_builder.Finish(&w_arr));

//     // Define Schema with the list_view type
//     auto schema = arrow::schema({
//         arrow::field("eventNumber", arrow::int32()),
//         arrow::field("mock_weights", arrow::list_view(arrow::float32()))
//     });

//     auto table = arrow::Table::Make(schema, {ev_arr, w_arr});
    
//     auto result = arrow::io::FileOutputStream::Open(output_file);
//     auto outfile = result.ValueOrDie();

//     verify_status(parquet::arrow::WriteTable(*table, m_pool, outfile, 1024));
//     verify_status(outfile->Close());
    
//     std::cout << "Success! Created " << output_file << " with variable-length ListView arrays." << std::endl;

//     return 0;
// }



// --- APPROACH 3: HYBRID INGESTION WITH BUFFER NORMALIZATION ---
#include <iostream>
#include <vector>
#include <string>
#include <memory>

// PODIO and EDM4hep headers
#include "podio/Frame.h"
#include "podio/ROOTReader.h"
#include "edm4hep/EventHeaderCollection.h"

// Apache Arrow and Parquet headers
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

void verify_status(const arrow::Status& status) {
    if (!status.ok()) {
        std::cerr << "Arrow Error: " << status.ToString() << std::endl;
        exit(1);
    }
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cout << "Usage: ./vector_converter <input_file.root> <output_file.parquet>" << std::endl;
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_file = argv[2];
    
    auto reader = podio::ROOTReader();
    try {
        reader.openFile(input_file);
    } catch (const std::exception& e) {
        std::cerr << "Critical Error: Could not open file: " << e.what() << std::endl;
        return 1;
    }

    auto m_pool = arrow::default_memory_pool();

    //INGESTION USING LISTVIEW ---
    arrow::Int32Builder event_num_builder(m_pool);
    auto value_builder = std::make_shared<arrow::FloatBuilder>(m_pool);
    arrow::ListViewBuilder weights_view_builder(m_pool, value_builder);

    size_t n_events = reader.getEntries("events");
    std::cout << "Step 1: Ingesting " << n_events << " events into ListView..." << std::endl;

    for (size_t i = 0; i < n_events; ++i) {
        auto frame = podio::Frame(reader.readEntry("events", i));
        const auto& event_headers = frame.get<edm4hep::EventHeaderCollection>("EventHeader");

        if (!event_headers.empty()) {
            for (const auto& h : event_headers) {
                verify_status(event_num_builder.Append(h.getEventNumber()));

                // Variable size logic (1, 2, or 3 weights)
                int64_t n_weights = (i % 3) + 1; 
                verify_status(weights_view_builder.Append(true, n_weights));
                
                std::vector<float> dynamic_weights;
                for(int j = 0; j < n_weights; ++j) {
                    dynamic_weights.push_back(1.0f / (j + 1)); 
                }
                verify_status(value_builder->AppendValues(dynamic_weights));
            }
        }
    }

    std::shared_ptr<arrow::Array> ev_arr, w_view_arr_raw;
    verify_status(event_num_builder.Finish(&ev_arr));
    verify_status(weights_view_builder.Finish(&w_view_arr_raw));

    //BUFFER NORMALIZATION (ListView -> List)
    // We manually rebuild the list to ensure the Offsets buffer is exactly N+1.
    std::cout << "Step 2: Normalizing buffers for Parquet compatibility..." << std::endl;
    
    auto list_view_arr = std::static_pointer_cast<arrow::ListViewArray>(w_view_arr_raw);
    auto final_value_builder = std::make_shared<arrow::FloatBuilder>(m_pool);
    arrow::ListBuilder final_list_builder(m_pool, final_value_builder);

    for (int64_t i = 0; i < list_view_arr->length(); ++i) {
        if (list_view_arr->IsNull(i)) {
            verify_status(final_list_builder.AppendNull());
        } else {
            verify_status(final_list_builder.Append());
            
            // Get the offset and length for this specific list element
            int64_t offset = list_view_arr->value_offset(i);
            int64_t length = list_view_arr->value_length(i);
            
            // Access the underlying values array 
            auto values = std::static_pointer_cast<arrow::FloatArray>(list_view_arr->values());
            
            // Get the raw pointer starting at the correct offset
            const float* raw_data_ptr = values->raw_values() + offset;
            
            // Append the values into the new, contiguous builder
            verify_status(final_value_builder->AppendValues(raw_data_ptr, length));
        }
    }

    std::shared_ptr<arrow::Array> final_weights_arr;
    verify_status(final_list_builder.Finish(&final_weights_arr));

    //TABLE GENERATION & WRITING
    auto schema = arrow::schema({
        arrow::field("eventNumber", arrow::int32()),
        arrow::field("mock_weights", arrow::list(arrow::float32()))
    });

    std::vector<std::shared_ptr<arrow::Array>> columns = {ev_arr, final_weights_arr};
    auto table = arrow::Table::Make(schema, columns);

    std::cout << "Step 3: Writing Table to Parquet..." << std::endl;
    auto result = arrow::io::FileOutputStream::Open(output_file);
    auto outfile = result.ValueOrDie();

    verify_status(parquet::arrow::WriteTable(*table, m_pool, outfile, 1024));
    verify_status(outfile->Close());

    std::cout << "Success! Created " << output_file << " with normalized ListArray." << std::endl;
    
    return 0;
}