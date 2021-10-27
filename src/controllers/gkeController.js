import _ from "lodash";
import monitoring from "@google-cloud/monitoring";
import {spawnNewNodeIfThresholdExceeded} from "../services/gkeService";
import {addDocumentWithoutId, createIndex} from "../services/elasticService";

const indexName = "gke-metric-data-predictive-4";

export async function trackGKE(req, res) {
    const projectId = 'qpefcs-course-project';
    const cpuFilter = 'metric.type="kubernetes.io/node/cpu/allocatable_utilization"';
    const cpuScaling = 25;
    const memoryScaling = 102941;

    const client = new monitoring.MetricServiceClient();
    const cluster_scaling_request = {
        name: client.projectPath(projectId),
        filter: cpuFilter,
        interval: {
            startTime: {
                // Limit results to the last 3 minutes
                seconds: Date.now() / 1000 - 60 * 4,
            },
            endTime: {
                seconds: Date.now() / 1000,
            },
        },
    };

    const [cpuTimeSeries] = await client.listTimeSeries(cluster_scaling_request);

    const memoryFilter = 'metric.type="kubernetes.io/node/memory/allocatable_utilization"';

    const memory_scaling_request = {
        name: client.projectPath(projectId),
        filter: memoryFilter,
        interval: {
            startTime: {
                // Limit results to the last 3 minutes
                seconds: Date.now() / 1000 - 60 * 4,
            },
            endTime: {
                seconds: Date.now() / 1000,
            },
        },
    };

    const [memoryTimeSeries] = await client.listTimeSeries(memory_scaling_request);

    const gkeInitNodes = _.map(cpuTimeSeries, function extractName(row) {
        return row.resource.labels["node_name"]
    })

    const nodesResourceUsage = _.mapValues(_.keyBy(_.map(gkeInitNodes, function extractUtil(node) {
        const cpuUsed = _.first(_.find(cpuTimeSeries, function matchingNode(row) {
            return row.resource.labels["node_name"] === node;
        }).points).value.doubleValue;
        const memoryUsed = _.first(_.find(memoryTimeSeries, function matchingNode(row) {
            return row.resource.labels["node_name"] === node;
        }).points).value.doubleValue;
        return {"name": node, "util": {"cpuUsed": cpuUsed * cpuScaling, "memoryUsed": memoryUsed * memoryScaling}}
    }), "name"), "util");


    await spawnNewNodeIfThresholdExceeded(nodesResourceUsage);
}

export async function plotGKE(req, res) {
    await createIndex(indexName)
    const projectId = 'qpefcs-course-project';
    const cpuFilter = 'metric.type="kubernetes.io/node/cpu/allocatable_utilization"';

    const client = new monitoring.MetricServiceClient();

    while (true) {
        const cluster_scaling_request = {
            name: client.projectPath(projectId),
            filter: cpuFilter,
            interval: {
                startTime: {
                    // Limit results to the last 3 minutes
                    seconds: Date.now() / 1000 - 60 * 4,
                },
                endTime: {
                    seconds: Date.now() / 1000,
                },
            },
        };

        const [cpuTimeSeries] = await client.listTimeSeries(cluster_scaling_request);
        const memoryFilter = 'metric.type="kubernetes.io/node/memory/allocatable_utilization"';

        const memory_scaling_request = {
            name: client.projectPath(projectId),
            filter: memoryFilter,
            interval: {
                startTime: {
                    // Limit results to the last 3 minutes
                    seconds: Date.now() / 1000 - 60 * 4,
                },
                endTime: {
                    seconds: Date.now() / 1000,
                },
            },
        };

        const [memoryTimeSeries] = await client.listTimeSeries(memory_scaling_request);

        const utilMetricInfoToLog = _.map(cpuTimeSeries, function mapToValue(row) {
            const memoryUtilRow = _.find(memoryTimeSeries, function fetchRow(memoryRow) {
                return row.resource.labels["node_name"] === memoryRow.resource.labels["node_name"]
            });
            const util = Math.max(_.first(memoryUtilRow.points).value.doubleValue, _.first(row.points).value.doubleValue)
            return {
                "node": row.resource.labels["node_name"],
                "metric": "utilization",
                "value": util,
                "@timestamp": Math.floor(new Date().getTime() / 1000)
            };
        })
        _.forEach(utilMetricInfoToLog, function indexToElasticsearch(utilInfo) {
            addDocumentWithoutId(indexName, utilInfo);
        })
        await new Promise(resolve => setTimeout(resolve, 2000));
    }
}

await Promise.all([plotGKE(), trackGKE()])
