# Data Governance with DataHub 


## 1. Data Lineage 
For **Batch** `jobs/publish_datahub_lineage.py` is responsible for publishing batch dataset lineage to DataHub, so that we have more visibility at each layer of the Medallion architecture(Bronze-> Silver-> Gold).

![Data_lineage_batch](/assets/Data_lineage_batch.png)
  
For **Streaming** `jobs/publish_streaming_lineage.py` is responsible for publishing streaming dataset lineage to DataHub : JSONL → Kafka raw topic → **Flink** → Kafka features topic →
  ClickHouse `feat_stream_30m`.

![Data_lineage_streaming](/assets/Data_lineage_streaming.png)

## 2. Data Quality Assurance 

- By clicking on a given dataset in the silver layer(Delta Lake), under `Quality`, we can see a set of predefined rules applied to a dataset such as every Silver table must have rows, a non-null primary/business key, no duplicate business keys after deduplication, and even business rules.

### **Silver** 
![silver_assertion](/assets/silver_assertion.png)

![silver_assertion_2](/assets/silver_assertion_2.png)


Clicking on `Business Rule`, we'll have more info regarding what kind of business rule is guaranteed

![silver_assertion_2.1](/assets/silver_assertion_2.1.png)

### **Gold**

![gold_data_assertion_1](/assets/gold_data_assertion_1.png)

**Data Contract**


![gold_data_assertion_2](/assets/gold_data_assertion_2.png)
