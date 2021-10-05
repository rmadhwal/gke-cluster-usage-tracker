import _ from "lodash";
import monitoring from '@google-cloud/monitoring';
import container from '@google-cloud/container';
import {addDocumentWithoutId, createIndex} from "./elasticService";

const indexName = "gke-metric-data";

function pad(n){return n<10 ? '0'+n : n}
export async function spawnNewNodeIfThresholdExceeded() {
    await createIndex(indexName)
    const projectId = 'qpefcs-course-project';
    const cpuFilter = 'metric.type="kubernetes.io/node/cpu/allocatable_utilization"';
    const client = new monitoring.MetricServiceClient();
    const clusterManagerClient = new container.v1.ClusterManagerClient();

    const clusterRequest = {
        projectId: projectId,
        zone: 'us-central1-c',
    };
    const [clusterInfoResponse] = await clusterManagerClient.listClusters(clusterRequest);
    const nodesUp = _.first(_.first(clusterInfoResponse.clusters).nodePools).initialNodeCount

    const cluster_scaling_request = {
        name: client.projectPath(projectId),
        filter: cpuFilter,
        interval: {
            startTime: {
                // Limit results to the last 3 minutes
                seconds: Date.now() / 1000 - 60 * 3,
            },
            endTime: {
                seconds: Date.now() / 1000,
            },
        },
    };

    const [cpuTimeSeries] = await client.listTimeSeries(cluster_scaling_request);
    const cpuMetricInfoToLog = _.map(cpuTimeSeries, function mapToValue(row) {
        return {"node": row.resource.labels["node_name"], "metric": "cpu utilization", "value": _.first(row.points).value.doubleValue, "@timestamp": _.first(row.points).interval.startTime.seconds};
    })

    const latestCpuUsageMetric = _.map(cpuTimeSeries, function mapToValue(row) {
        return _.first(row.points).value.doubleValue;
    })

    const newNodeCPUThreshhold = 0.75 * latestCpuUsageMetric.length
    const newNodeMemoryThreshhold = 0.75 * latestCpuUsageMetric.length

    cpuTimeSeries.forEach(data => {
        console.log(`${JSON.stringify(data.resource.labels)}:`);
        data.points.forEach(point => {
            console.log(JSON.stringify(point.value));
        });
    });

    const memoryFilter = 'metric.type="kubernetes.io/node/memory/allocatable_utilization"';

    const memory_scaling_request = {
        name: client.projectPath(projectId),
        filter: memoryFilter,
        interval: {
            startTime: {
                // Limit results to the last 3 minutes
                seconds: Date.now() / 1000 - 60 * 3,
            },
            endTime: {
                seconds: Date.now() / 1000,
            },
        },
    };

    const [memoryTimeSeries] = await client.listTimeSeries(memory_scaling_request);
    const memoryMetricInfoToLog = _.map(cpuTimeSeries, function mapToValue(row) {
        return {"node": row.resource.labels["node_name"], "metric": "memory utilization", "value": _.first(row.points).value.doubleValue, "@timestamp": _.first(row.points).interval.startTime.seconds};
    })
    const latestMemoryUsageMetric = _.map(memoryTimeSeries, function mapToValue(row) {
        return _.first(row.points).value.doubleValue;
    })

    if(_.sum(latestCpuUsageMetric) >= newNodeCPUThreshhold || _.sum(latestMemoryUsageMetric) >= newNodeMemoryThreshhold) {
        await clusterManagerClient.setNodePoolSize({nodePoolId: "default-pool", nodeCount: nodesUp + 1, zone: 'us-central1-c', projectId: "qpefcs-course-project", clusterId: "flfk-cluster"})
    }

    _.forEach(cpuMetricInfoToLog, function indexToElasticsearch(cpuInfo) {
        addDocumentWithoutId(indexName, cpuInfo);
    })

    _.forEach(memoryMetricInfoToLog, function indexToElasticsearch(memoryInfo) {
        addDocumentWithoutId(indexName, memoryInfo);
    })
}


    /**
     * TODO(developer): Uncomment and edit the following lines of code.
     */


//Using BigQuery

// let date_ob = new Date();
// let date = pad(date_ob.getDate());
// let month = pad(date_ob.getMonth() + 1);
// let year = date_ob.getFullYear();
//
// const kubernetes = google.gkehub()

// const bigqueryClient = new BigQuery();
// // const sqlQuery = `SELECT *
// //                   FROM \`qpefcs-course-project.k8s_data.gke_cluster_resource_usage\`
// //                   WHERE DATE (_PARTITIONTIME) = "${year}-${month}-${date}" LIMIT 1000`;
// const sqlQuery = `SELECT *
//                   FROM \`qpefcs-course-project.k8s_data.gke_cluster_resource_usage\`
//                   WHERE DATE (_PARTITIONTIME) = "2021-10-03" LIMIT 1000`;
//
// const options = {
//     query: sqlQuery,
//     // Location must match that of the dataset(s) referenced in the query.
//     location: 'US',
// };
//
// // Run the query
// const [rows] = await bigqueryClient.query(options);
//
// const pytorch_rows = _.filter(rows, function pytorchRow(row) {
//     return row.labels;
// })
//
// console.log('Query Results:');
// rows.forEach(row => {
//     const url = row;
//     const viewCount = row['view_count'];
//     console.log(`url: ${url}, ${viewCount} views`);
// });

