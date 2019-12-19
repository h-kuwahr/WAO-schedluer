package minimizepower

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	pb "tensorflow_serving/apis"
	tfcoreframework "tensorflow/core/framework"
	"google.golang.org/grpc"
	"context"
	"strconv"
	"time"
	"math"
	"github.com/k-sone/ipmigo"
	"os"
	"sync"
	"encoding/csv"

	"k8s.io/api/core/v1"
	"k8s.io/klog"
	"k8s.io/apimachinery/pkg/runtime"
	//"k8s.io/apimachinery/pkg/labels"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/client-go/tools/clientcmd"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//metricsapi "k8s.io/metrics/pkg/apis/metrics"
	v1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)



type Model_list struct{
	handle framework.FrameworkHandle
	Models map[string]Model
	Wind map[string]float32
	Ambient map[string]float32
	Ambient_timestamp map[string]time.Time
	Ambient_buf float32
	Power_cache map[key]float32
	Starting_pod_list map[string]float64
	sync.Mutex
}

type key struct{
	server, cpu, ambient string
}

type Model struct{
	Model_name string
	Inputs_name string
	Outputs_name string
	Num_inputs int
	Num_outputs int
	Average []float32
	Deviation []float32
}

var _ = framework.ScorePlugin(&Model_list{})

const MinimizePowerName = "wao-score-plugin"

func(m *Model_list) Name() string{
	return MinimizePowerName
}

func(m *Model) Normalize32(Input_s []float32)(error) {
  if len(m.Average) != m.Num_inputs || len(m.Deviation) != m.Num_inputs || len(Input_s) != m.Num_inputs{
    return fmt.Errorf("Mismatch num of inputs to model array")
  }
  for i := 0; i < m.Num_inputs; i++{
      Input_s[i] = (Input_s[i] - m.Average[i])/m.Deviation[i]
  }
  return nil
}

func(m *Model) BackPlaneWind(cpu float32, temp float32)(float32, error){
  if m.Model_name == "m4-prediction"{
		var min_wind float32
		var linear float32
		if cpu < 50{
			min_wind = 1.2
		}else{
			min_wind = 1.3
		}

		if cpu < 6.25{
			linear = 0.0
		}else if cpu < 12.5{
			linear = 1/75
		}else if cpu < 18.75{
			linear = 2/75
		}else if cpu < 93.25{
			linear = 0.04
		}else{
			linear = 7/150
		}

    if temp > 12{
        return min_wind + linear * (temp - 12), nil
    }else{
        return min_wind, nil
	  }
	}else if m.Model_name == "m2-prediction"{
		var min_wind float32
		var linear float32
		if cpu < 50{
			min_wind = 1.6
		}else{
			min_wind = 1.7
		}

		if cpu < 58.3333{
			linear = 0.025
		}else{
			linear = 7/160
		}

    if temp > 11{
        return min_wind + linear * (temp - 11), nil
    }else{
        return min_wind, nil
	  }
	}else{
		    return 0, fmt.Errorf("model not found")
	}
}

func(m *Model) PowerPredict(Input []float32, servingAddress string)([]float32, error){
	request := &pb.PredictRequest{
              ModelSpec: &pb.ModelSpec{
                Name: m.Model_name,
                SignatureName: "serving_default",
              },
              Inputs: map[string]*tfcoreframework.TensorProto{
                m.Inputs_name: &tfcoreframework.TensorProto{
                  Dtype: tfcoreframework.DataType_DT_FLOAT,
                  TensorShape: &tfcoreframework.TensorShapeProto{
                    Dim: []*tfcoreframework.TensorShapeProto_Dim{
                      &tfcoreframework.TensorShapeProto_Dim{
                        Size: int64(1),
                      },
                      &tfcoreframework.TensorShapeProto_Dim{
                        Size: int64(m.Num_inputs),
                      },
                    },
                  },
                  FloatVal: Input,
                },
              },
      }
      conn, err := grpc.Dial(servingAddress, grpc.WithInsecure())
      if err != nil {
	      return nil, fmt.Errorf("Cannot connect to the grpc server: %v\n", err)
      }
      defer conn.Close()

      client := pb.NewPredictionServiceClient(conn)

      resp, err := client.Predict(context.Background(), request)
      if err != nil {
	      return nil, fmt.Errorf("%v\n", err)
      }
      return resp.Outputs[m.Outputs_name].FloatVal, nil
}

func GetNodeMetrics(node string)(*v1beta1.NodeMetrics, error){
        var kubeconfig, master string //empty, assuming inClusterConfig
	var err1 error
        var nodemetrics *v1beta1.NodeMetrics
        //var nodemetricslist *v1beta1.NodeMetricsList
        config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
        if err != nil{
                return nil, err
        }
        mc, err := metrics.NewForConfig(config)
	if err != nil {
                return nil, err
        }
        nm := mc.MetricsV1beta1().NodeMetricses()
        nodemetrics, err1 = nm.Get(node, metav1.GetOptions{})
        //nodemetricslist, err2 = nm.List(metav1.ListOptions{})
        //convertedList := &metricsapi.NodeMetricsList{}
        //err2 = v1beta1.Convert_v1beta1_NodeMetricsList_To_metrics_NodeMetricsList(nodemetricslist, convertedList, nil)
        if klog.V(2){
		klog.Infof("Get -> %v, err -> %v", nodemetrics, err1)
	}
	return nodemetrics, err1
        //klog.Infof("List -> %v, err -> %v", convertedList, err2)
}

func GetPodMetrics(pod string, namespace string)(*v1beta1.PodMetrics, error){
	 var kubeconfig, master string //empty, assuming inClusterConfig
	 var podmetrics *v1beta1.PodMetrics
	 config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	 if err != nil{
		 return nil, err
	 }
	 mc, err := metrics.NewForConfig(config)
	 if err != nil {
		 return nil, err
	 }
	 pm := mc.MetricsV1beta1().PodMetricses(namespace)
	 podmetrics, err = pm.Get(pod, metav1.GetOptions{})
	 if klog.V(2){
		 klog.Infof("Get -> %v, err -> %v", podmetrics, err)
	 }
	 return podmetrics, err
}

func GetNodeTemperature(node_address string) (float32, error){
	c, err := ipmigo.NewClient(ipmigo.Arguments{
		Version:       ipmigo.V2_0,
		Address:       node_address,
		Timeout:       2 * time.Second,
		Retries:       1,
		Username:      "admin",
		Password:      "admin",
		CipherSuiteID: 3,
	})
	if err != nil {
		return 0, err
	}

	if err := c.Open(); err != nil {
		return 0, err
	}
	defer c.Close()

	// Get sensor records
	records, err := ipmigo.SDRGetRecordsRepo(c, func(id uint16, t ipmigo.SDRType) bool {
		return id == 0x0001
		//return t == ipmigo.SDRTypeFullSensor || t == ipmigo.SDRTypeCompactSensor
	})
	if err != nil {
		return 0, err
	}

	r := records[0]
	// Get sensor reading
	var run, num uint8
	switch s := r.(type) {
	case *ipmigo.SDRFullSensor:
		run = s.OwnerLUN
		num = s.SensorNumber
	case *ipmigo.SDRCompactSensor:
		run = s.OwnerLUN
		num = s.SensorNumber
	}
	gsr := &ipmigo.GetSensorReadingCommand{
		RsLUN:        run,
		SensorNumber: num,
	}
	exec_err, ok := c.Execute(gsr).(*ipmigo.CommandError)
	if exec_err != nil && !ok {
		return 0, exec_err
	}

	// Output sensor reading
	var convf func(uint8) float64
	switch s := r.(type) {
	case *ipmigo.SDRFullSensor:
		convf = func(r uint8) float64 { return s.ConvertSensorReading(r) }
	}

	if exec_err != nil {
		return 0, exec_err
	} else {
		if gsr.IsValid() {
			return float32(convf(gsr.SensorReading)), nil
		}else{
			return 0, fmt.Errorf("gsr is not invalid")
		}
	}
	// fmt.Println(reading)
	// fmt.Println(status)
	//fmt.Printf(format, sname, stype, reading, units, status)
}

func(m *Model_list) SetCache(k key, value float32) {
	m.Lock()
	m.Power_cache[k] = value
	m.Unlock()
}

func(m *Model_list) GetCache(k key) (float32, bool){
	m.Lock()
	value, ok:= m.Power_cache[k]
	m.Unlock()
	return value, ok
}

func(m *Model_list) DeleteKey(k string){
	m.Lock()
	defer m.Unlock()
	delete(m.Starting_pod_list, k)
}

func(m *Model_list) ReadMap(k string) (v float64, ok bool){
	m.Lock()
	defer m.Unlock()
	v, ok = m.Starting_pod_list[k]
	return
}

func(m *Model_list) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status){
//	if value, exist := pod.Labels["prediction"]; exist && value == "true"{
		nodeInfo, err := m.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
		if klog.V(2){
			klog.Infof("OwnerReferences: %v", pod.OwnerReferences)
		}
		if err != nil {
			return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
		}

		node := nodeInfo.Node()
		if node	== nil {
			return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Node is nil"))
		}

		NodeMetrics, Metrics_err := GetNodeMetrics(node.Name)
		if Metrics_err != nil{
			return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Metrics of node %s cannot be got", nodeName))
		}
		NodeMetrics_CPU := NodeMetrics.Usage["cpu"]
		buf_dec := NodeMetrics_CPU.AsDec()
		NodeCPU_usage, _ := strconv.ParseFloat(buf_dec.String(), 32)
		Node_resource := node.Status.Capacity["cpu"]
		buf_dec = Node_resource.AsDec()
		NodeCPU_capacity, _ := strconv.ParseFloat(buf_dec.String(), 32)
		model_name := node.Labels["predict_service"]
		pods := nodeInfo.Pods()
		for _, pod := range pods{
			if v, ok := m.ReadMap(pod.Name); ok{
				var pod_usage float64
				pod_metrics, err := GetPodMetrics(pod.Name, pod.Namespace)
				if err == nil {
					for _, container := range pod_metrics.Containers{
						container_CPU := container.Usage["cpu"]
						buf_dec = container_CPU.AsDec()
						container_usage, _ := strconv.ParseFloat(buf_dec.String(), 32)
						pod_usage += container_usage
					}
				}
				if pod_usage < v * 0.03 {
					NodeCPU_usage += pod_usage
				} else {
					m.DeleteKey(pod.Name)
				}
			}
		}

		pod_resource_limits := pod.Spec.Containers[0].Resources.Limits["cpu"]
		buf_dec = pod_resource_limits.AsDec()
		Limits_core, err_limit := strconv.ParseFloat(buf_dec.String(), 32)
		//klog.Infof("Pod %s limit: AsDec -> %v, AsDec.String() -> %v, strconv -> %v", pod.Name, buf_dec, buf_dec.String(), Limits_core)
		pod_resource_requests := pod.Spec.Containers[0].Resources.Requests["cpu"]
		buf_dec = pod_resource_requests.AsDec()
		Requests_core, err_request := strconv.ParseFloat(buf_dec.String(), 32)
		//klog.Infof("Pod %s request: AsDec -> %v, AsDec.String() -> %v, strconv -> %v", pod.Name, buf_dec, buf_dec.String(), Requests_core)
		now_usage := float32(NodeCPU_usage/NodeCPU_capacity)*float32(100)
		//klog.Infof("Node %s usage: %v", nodeName, now_usage)
		var next_usage float32
		if Requests_core != 0 && err_request == nil{
			next_usage = float32((NodeCPU_usage + Requests_core)/NodeCPU_capacity)*float32(100)
		}else if Limits_core != 0 && err_limit == nil{
			next_usage = float32((NodeCPU_usage + Limits_core)/NodeCPU_capacity)*float32(100)
		}else{
			//When non request and limit, the pod is not scored
			klog.Infof("Pod %s deos not define requested and limited resources", pod.Name)
			return int64(-1), nil
		}
		model, ok := m.Models[model_name]
		if !ok {
			klog.Infof("Model %s is not registerd", model_name)
			return int64(-1), nil
		}
		Input_now, Input_next := make([]float32, model.Num_inputs), make([]float32, model.Num_inputs)
		Input_now[1], Input_next[1] = (now_usage - model.Average[1])/model.Deviation[1], (next_usage - model.Average[1])/model.Deviation[1]
		if next_usage > float32((NodeCPU_capacity - 1.0)/NodeCPU_capacity)*float32(100){
			return int64(-1), nil
		}

		var temp float32
		if m.Ambient_timestamp[nodeName].IsZero() || int(time.Since(m.Ambient_timestamp[nodeName]).Minutes()) >= 120 {
			temp, err = GetNodeTemperature(node.Labels["ipmi"]+":623")
			if err != nil{
				temp = m.Ambient_buf
			} else {
				m.Ambient_buf = temp
				m.Ambient_timestamp[nodeName] = time.Now()
			}
			//klog.Infof("Node %s ambient: %v ℃", nodeName, temp)
			Input_now[0], Input_next[0] = (temp - model.Average[0])/model.Deviation[0], (temp - model.Average[0])/model.Deviation[0]
		}else{
			temp = m.Ambient[nodeName]
			Input_now[0], Input_next[0] = (m.Ambient[nodeName] - model.Average[0])/model.Deviation[0], (m.Ambient[nodeName] - model.Average[0])/model.Deviation[0]
		}

		var result_now, result_next float32
		if v, ok := m.GetCache(key{nodeName, fmt.Sprintf("%.1f", now_usage), fmt.Sprintf("%.1f", temp)}); ok{
			result_now = v
		}else{
			if v, ok := m.Wind[node.Name]; ok{
				Input_now[2], err = model.BackPlaneWind(now_usage, temp)
				Input_now[2] += v
				Input_now[2] = (Input_now[2] - model.Average[2])/model.Deviation[2]
			}else{
				return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Wind of node %s cannot be got", nodeName))
			}
			result_buf, err := model.PowerPredict(Input_now, "prediction-service.default.svc.cluster.local:8500")
			if err != nil{
				return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Power of node %s cannot be got", nodeName))
			}
			result_now = result_buf[0]
			m.SetCache(key{nodeName, fmt.Sprintf("%.1f", now_usage), fmt.Sprintf("%.1f", temp)}, result_now)
		}

		if v, ok := m.GetCache(key{nodeName, fmt.Sprintf("%.1f", next_usage), fmt.Sprintf("%.1f", temp)}); ok{
			result_next = v
		}else{
			if v, ok := m.Wind[node.Name]; ok{
				Input_next[2], err = model.BackPlaneWind(next_usage, temp)
				Input_next[2] += v
				Input_next[2] = (Input_next[2] - model.Average[2])/model.Deviation[2]
			}else{
				return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Wind of node %s cannot be got", nodeName))
			}
			result_buf, err := model.PowerPredict(Input_next, "prediction-service.default.svc.cluster.local:8500")
			if err != nil{
				return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Power of node %s cannot be got", nodeName))
			}
			result_next = result_buf[0]
			m.SetCache(key{nodeName, fmt.Sprintf("%.1f", next_usage), fmt.Sprintf("%.1f", temp)}, result_next)
		}
		//klog.Infof("Node %s power: now -> %v, next -> %v", nodeName, result_now, result_next)
		score := result_next - result_now
		klog.Infof("Node %s -> score: %f", nodeName, score)

		return int64(score), nil
//	} else {
//		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Pod %s is not deploied by minimize priority", pod.Name))
//	}
}

func (m *Model_list) ScoreExtensions() framework.ScoreExtensions {
	return m
}

func (m *Model_list) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status{
	klog.Infof("ScoreList: %v", scores)
	highest := int64(0)
	lowest := int64(math.MaxInt64)
	for _, score := range scores {
		if score.Score > highest && score.Score >= 0{
			highest = score.Score
		}
		if score.Score < lowest && score.Score >= 0{
			lowest = score.Score
		}
	}
	NodeScoreMax := framework.MaxNodeScore
	for node, score := range scores {
		if score.Score < 0{
			scores[node].Score = 0
		}else if highest != lowest {
			scores[node].Score = int64(NodeScoreMax - (NodeScoreMax*(score.Score - lowest)/(highest - lowest)))
		}else{
			scores[node].Score = 10
		}
	}
	pod_resource_limits := p.Spec.Containers[0].Resources.Limits["cpu"]
	buf_dec := pod_resource_limits.AsDec()
	Limits_core, err_limit := strconv.ParseFloat(buf_dec.String(), 32)
	pod_resource_requests := p.Spec.Containers[0].Resources.Requests["cpu"]
	buf_dec = pod_resource_requests.AsDec()
	Requests_core, err_request := strconv.ParseFloat(buf_dec.String(), 32)
	if Requests_core != 0 && err_request == nil{
		m.Starting_pod_list[p.Name] = Requests_core
	}else if Limits_core != 0 && err_limit == nil{
		m.Starting_pod_list[p.Name] = Limits_core
	}
	return nil
}

func New(_ *runtime.Unknown, h framework.FrameworkHandle) (framework.Plugin, error) {
	var m Model_list
	m.handle = h
	buf, err := ioutil.ReadFile("/usr/local/bin/model_config.yaml")
	if err != nil {
				return nil, err
	}
	err = yaml.Unmarshal(buf, &m)
	if err != nil {
				return nil, err
	}
	file, err := os.Open("/usr/local/bin/wind_konohana.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	m.Wind = make(map[string]float32)
	m.Power_cache = make(map[key]float32)
	m.Ambient = make(map[string]float32)
	m.Ambient_timestamp = make(map[string]time.Time)
	m.Starting_pod_list = make(map[string]float64)
	_, _ = reader.Read()
	nodeInfos := m.handle.SnapshotSharedLister().NodeInfos()
	for {
		line, err := reader.Read()
		if err != nil{
			break
		}
		buf, err := strconv.ParseFloat(line[1], 32)
		if err != nil{
			return nil, err
		}
		m.Wind[line[0]] = float32(buf)
		nodeInfo, err := nodeInfos.Get(line[0])
		if err == nil {
			node := nodeInfo.Node()
			if node != nil{
				temp, err := GetNodeTemperature(node.Labels["ipmi"]+":623")
				if err == nil{
					m.Ambient[line[0]] = temp
					m.Ambient_buf = temp
					m.Ambient_timestamp[line[0]] = time.Now()
				}
			}
		}
	}
	return &m, nil
}
