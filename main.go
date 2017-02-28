// Package main is the main entrypoint to the worker cleaner
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	redis "gopkg.in/redis.v4"

	"github.com/pkg/errors"
	"k8s.io/client-go/1.5/kubernetes"
	"k8s.io/client-go/1.5/pkg/api"
	"k8s.io/client-go/1.5/tools/clientcmd"
)

const (
	resqueWorkerKey      = "resque:worker"
	resqueWorkersKey     = "resque:workers"
	resqueQueueKey       = "resque:queue"
	resqueQueuesKey      = "resque:queues"
	resqueFailedQueueKey = "resque:failed"
	statProcessedKey     = "resque:stat:processed"
	statFailedKey        = "resque:stat:failed"
)

var (
	blacklistedResqueQueues []string
	namespace               string
	podRoleSelector         string
	podAppSelector          string
	run                     string
	matchPrefix             string
)

func init() {
	blacklistedResqueQueues = strings.Split(os.Getenv("BLACKLISTED_QUEUES"), ",")
	namespace = os.Getenv("NAMESPACE")
	podAppSelector = os.Getenv("POD_APP")
	run = os.Getenv("RUN")
	matchPrefix = os.Getenv("MATCH_PREFIX")
}

func main() {
	log.Printf("info - " + "Kubernetes-Resque Worker Cleanup Service")

	kubeClient, err := getKubernetesClient()
	if err != nil {
		log.Fatalf(errors.Wrap(err, "Unable to create kubernetes client").Error())
	}
	redisClient := getRedisClient()

	log.Printf("info - "+"starting cleanup at %v...", time.Now())

	kubernetesWorkers, err := getWorkersFromKubernetes(namespace, kubeClient)
	if err != nil {
		log.Fatalf(errors.Wrap(err, "Unable to get workers from kubernetes").Error())

	}

	redisWorkers, err := getWorkersFromRedis(redisClient)
	if err != nil {
		log.Fatalf(errors.Wrap(err, "Unable to get workers from redis").Error())
	}

	deadRedisWorkers := redisWorkersNotInKubernetes(redisWorkers, kubernetesWorkers)

	log.Printf("info - "+"Found %d kubernetes workers", len(kubernetesWorkers))
	log.Printf("info - "+"Found %d redis workers", len(redisWorkers))
	log.Printf("info - "+"Found %d dead redis workers", len(deadRedisWorkers))

	for _, redisWorker := range deadRedisWorkers {
		log.Printf("info - "+"Dead worker: %s", redisWorker.name)
		if run == "1" {
			if err := removeDeadRedisWorker(redisClient, redisWorker); err != nil {
				log.Fatalf(errors.Wrap(err, fmt.Sprintf("Unable to delete %s", redisWorker)).Error())
			}
		}
	}

	// failedResqueJobs, err := getDirtyExitFailedJobsFromRedis(redisClient)
	// if err != nil {
	// 	log.Fatalf(errors.Wrap(err, "Unable to get failed jobs from redis").Error())
	// }

	// log.Printf("info - "+"Found %d failed resque jobs", len(failedResqueJobs))

	// for _, failedResqueJob := range failedResqueJobs {
	// 	log.Printf("info - "+"failed job: %s", failedResqueJob.name)
	// 	if run == "1" {
	// 		if err := removeFailedResqueJob(redisClient, failedResqueJob); err != nil {
	// 			log.Fatalf(errors.Wrap(err, "Unable to retry failed resque job").Error())
	// 		}
	// 	}
	// }

	log.Printf("info - "+"finished cleanup at %v...", time.Now())
}

func getKubernetesClient() (*kubernetes.Clientset, error) {
	k8sConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG_FILE"))
	if err != nil {
		return nil, err
	}
	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, err
	}
	return k8sClient, nil
}

func getWorkersFromKubernetes(namespace string, client *kubernetes.Clientset) ([]string, error) {
	podlist, err := client.Pods(namespace).List(api.ListOptions{})
	if err != nil {
		return nil, err
	}

	var podNames []string
	for _, pod := range podlist.Items {
		if pod.Labels["app"] == podAppSelector && pod.Namespace == namespace {
			podNames = append(podNames, pod.Name)
		}
	}

	return podNames, nil
}

func getWorkersFromRedis(redisClient *redis.Client) ([]redisWorker, error) {
	workers, err := redisClient.SMembers(resqueWorkersKey).Result()
	if err != nil {
		return []redisWorker{}, errors.Wrap(err, "Failed to get resque workers")
	}

	var redisWorkers []redisWorker

	for _, worker := range workers {
		workerName := strings.SplitN(worker, ":", 2)[0]

		if strings.HasPrefix(workerName, matchPrefix) {
			redisWorkers = append(redisWorkers, redisWorker{workerName, worker})
		}
	}

	return redisWorkers, nil
}

type redisWorker struct {
	name string
	info string
}

func getDirtyExitFailedJobsFromRedis(redisClient *redis.Client) ([]redisWorker, error) {
	failedJobsLength, err := redisClient.LLen(resqueFailedQueueKey).Result()
	if err != nil {
		return []redisWorker{}, errors.Wrap(err, "Failed to get failed resque jobs length")
	}

	failedJobs, err := redisClient.LRange(resqueFailedQueueKey, 0, failedJobsLength).Result()
	if err != nil {
		return []redisWorker{}, errors.Wrap(err, "Failed to get failed resque jobs")
	}

	var failedResqueJobs []redisWorker
	for _, failedJob := range failedJobs {
		var job failedResqueJob
		if err := json.Unmarshal([]byte(failedJob), &job); err != nil {
			continue
		}

		if job.Exception == "Resque::DirtyExit" {
			failedResqueJobs = append(failedResqueJobs, redisWorker{failedJob, failedJob})
		}
	}

	return failedResqueJobs, nil
}

type failedResqueJob struct {
	Exception string `json:"exception"`
}

func redisWorkersNotInKubernetes(redisWorkers []redisWorker, kubernetesWorkers []string) []redisWorker {
	var deadRedisWorkers []redisWorker

OUTER:
	for _, redisWorker := range redisWorkers {
		for _, kubernetesWorker := range kubernetesWorkers {
			if kubernetesWorker == redisWorker.name {
				continue OUTER
			}
		}

		deadRedisWorkers = append(deadRedisWorkers, redisWorker)
	}

	return deadRedisWorkers
}

func removeDeadRedisWorker(redisClient *redis.Client, redisWorker redisWorker) error {
	// bytes, err := redisClient.Get(fmt.Sprintf("%s:%s", resqueWorkerKey, redisWorker.info)).Bytes()
	// if err != nil && err != redis.Nil {
	// 	return errors.Wrap(err, fmt.Sprintf("Error getting %s from redis", redisWorker))
	// }
	// if err != nil && err == redis.Nil {
	// 	// Redis key not present so the issue probably corrected itself
	// 	return nil
	// }

	// if err := retryDeadWorker(redisClient, bytes); err != nil {
	// 	return errors.Wrap(err, fmt.Sprintf("Unable to retry %s", redisWorker))
	// }

	redisClient.Pipelined(func(pipe *redis.Pipeline) error {
		pipe.SRem(resqueWorkersKey, redisWorker.info)
		pipe.Del(fmt.Sprintf("%s:%s", resqueWorkerKey, redisWorker.info))
		pipe.Del(fmt.Sprintf("%s:%s:started", resqueWorkerKey, redisWorker.info))
		pipe.Del(fmt.Sprintf("%s:%s:shutdown", resqueWorkerKey, redisWorker.info))
		pipe.Del(fmt.Sprintf("%s:%s", statProcessedKey, redisWorker.info))
		pipe.Del(fmt.Sprintf("%s:%s", statFailedKey, redisWorker.info))
		return nil
	})

	return nil
}

func removeFailedResqueJob(redisClient *redis.Client, redisWorker redisWorker) error {
	if err := retryDeadWorker(redisClient, []byte(redisWorker.info)); err != nil {
		return errors.Wrap(err, fmt.Sprintf("Unable to retry %s", redisWorker.info))
	}

	redisClient.Pipelined(func(pipe *redis.Pipeline) error {
		pipe.LRem(resqueFailedQueueKey, 1, redisWorker.info)
		return nil
	})

	return nil
}

func retryDeadWorker(redisClient *redis.Client, workerData []byte) error {
	resqueJob, err := newResqueJob(workerData)
	if err != nil {
		return errors.Wrap(err, "Unable to deserialize job")
	}

	log.Printf("info - "+"Going to retry job on queue: %s, with payload: %s", resqueJob.Queue, resqueJob.Payload)

	if err := redisClient.SAdd(resqueQueuesKey, resqueJob.Queue).Err(); err != nil {
		return errors.Wrap(err, fmt.Sprintf("Unable to create resque queue %s", resqueJob.Queue))
	}

	resqueJobJSON, err := resqueJob.Payload.MarshalJSON()
	if err != nil {
		return errors.Wrap(err, "Could not serialize job payload")
	}

	if err = redisClient.RPush(fmt.Sprintf("%s:%s", resqueQueueKey, resqueJob.Queue), string(resqueJobJSON)).Err(); err != nil {
		return errors.Wrap(err, "Failed to insert job into resque queue")
	}

	return nil
}

func newResqueJob(data []byte) (resqueJob, error) {
	var j resqueJob
	err := json.Unmarshal(data, &j)
	return j, err
}

type resqueJob struct {
	Queue   string `json:"queue"`
	Payload json.RawMessage
}

func getRedisClient() *redis.Client {
	addr := fmt.Sprintf("%s:%s", os.Getenv("REDIS_SERVICE_HOST"), os.Getenv("REDIS_SERVICE_PORT"))
	return redis.NewClient(&redis.Options{
		Addr: addr,
	})
}
