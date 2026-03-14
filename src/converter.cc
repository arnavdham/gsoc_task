#include <iostream>
#include <vector>
#include <string>

#include "podio/Frame.h"
#include "podio/ROOTReader.h"
#include "edm4hep/EventHeaderCollection.h"

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
    //Accept command-line arguments for input and output
    if (argc < 3) {
        std::cout << "Usage: ./converter <input_file.root> <output_file.parquet>" << std::endl;
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_file = argv[2];
    auto reader = podio::ROOTReader();
    
    try {
        reader.openFile(input_file);
    } catch (const std::exception& e) {
        std::cerr << "Critical Error: Could not open file. Details: " << e.what() << std::endl;
        return 1;
    }

    auto m_pool = arrow::default_memory_pool();

    // Builders for specific EventHeader fields
    arrow::Int32Builder event_num_builder(m_pool);
    arrow::Int32Builder run_num_builder(m_pool);
    arrow::UInt64Builder timestamp_builder(m_pool);
    arrow::FloatBuilder weight_builder(m_pool);

    size_t n_events = reader.getEntries("events");
    std::cout << "Converting " << n_events << " EventHeaders to Parquet..." << std::endl;

    for (size_t i = 0; i < n_events; ++i) {
        auto frame = podio::Frame(reader.readEntry("events", i));
        
        // Use PODIO to read EventHeader collections
        const auto& event_headers = frame.get<edm4hep::EventHeaderCollection>("EventHeader");

        if (event_headers.isValid() && !event_headers.empty()) {
            const auto& h = event_headers[0];
            
            //Extract fields and convert to Arrow format
            verify_status(event_num_builder.Append(h.getEventNumber()));
            verify_status(run_num_builder.Append(h.getRunNumber()));
            verify_status(timestamp_builder.Append(h.getTimeStamp()));
            verify_status(weight_builder.Append(h.getWeight()));
        }
    }

    // Finalize Arrays
    std::shared_ptr<arrow::Array> ev_arr, run_arr, ts_arr, w_arr;
    verify_status(event_num_builder.Finish(&ev_arr));
    verify_status(run_num_builder.Finish(&run_arr));
    verify_status(timestamp_builder.Finish(&ts_arr));
    verify_status(weight_builder.Finish(&w_arr));

    //Define Columnar Schema
    auto schema = arrow::schema({
        arrow::field("eventNumber", arrow::int32()),
        arrow::field("runNumber", arrow::int32()),
        arrow::field("timeStamp", arrow::uint64()),
        arrow::field("weight", arrow::float32())
    });

    //Write converted data to Parquet format
    auto table = arrow::Table::Make(schema, {ev_arr, run_arr, ts_arr, w_arr});
    auto result = arrow::io::FileOutputStream::Open(output_file);
    auto outfile = result.ValueOrDie();

    verify_status(parquet::arrow::WriteTable(*table, m_pool, outfile, 1024));
    verify_status(outfile->Close());
    
    std::cout << "Success! Created " << output_file << " with " << ev_arr->length() << " rows." << std::endl;

    return 0;
}