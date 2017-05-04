package google

import (
	"fmt"

	"github.com/hashicorp/terraform/helper/schema"
	"google.golang.org/api/dataflow/v1b3"
)

func resourceDataflowJob() *schema.Resource {
	return &schema.Resource{
		Create: resourceDataflowJobCreate,
		Read:   resourceDataflowJobRead,
		Delete: resourceDataflowJobDelete,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"gcs_path": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"temp_location": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"zone": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},

			"max_workers": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				Default:  1,
				ForceNew: true,
			},

			"parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				ForceNew: true,
			},

			"on_delete": &schema.Schema{
				Type:         schema.TypeString,
				ValidateFunc: validateAllowedStringValue([]string{"cancel", "drain"}),
				Optional:     true,
				Default:      "drain",
				ForceNew:     true,
			},

			"project": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},

			"state": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceDataflowJobCreate(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	jobName := d.Get("name").(string)
	gcsPath := d.Get("gcs_path").(string)
	tempLocation := d.Get("temp_location").(string)
	zone := d.Get("zone").(string)
	maxWorkers := d.Get("max_workers").(int)
	params := expandStringMap(d.Get("parameters").(map[string]interface{}))

	templateService := dataflow.NewProjectsTemplatesService(config.clientDataflow)

	env := dataflow.RuntimeEnvironment{
		TempLocation: tempLocation,
		Zone:         zone,
		MaxWorkers:   int64(maxWorkers),
	}

	request := dataflow.CreateJobFromTemplateRequest{
		JobName:     jobName,
		GcsPath:     gcsPath,
		Parameters:  params,
		Environment: &env,
	}

	job, err := templateService.Create(project, &request).Do()
	if err != nil {
		return err
	}

	d.SetId(job.Id)
	d.Set("state", job.CurrentState)

	return nil
}

func resourceDataflowJobRead(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	id := d.Id()

	job, err := config.clientDataflow.Projects.Jobs.Get(project, id).Do()
	if err != nil {
		return err
	}

	d.Set("state", job.CurrentState)

	return nil
}

func resourceDataflowJobDelete(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	id := d.Id()
	requestedState, err := mapOnDelete(d.Get("on_delete").(string))
	if err != nil {
		return err
	}

	job := &dataflow.Job{
		RequestedState: requestedState,
	}

	_, err = config.clientDataflow.Projects.Jobs.Update(project, id, job).Do()
	if err != nil {
		return err
	}

	return nil
}

func expandStringMap(m map[string]interface{}) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		result[k] = v.(string)
	}
	return result
}

func mapOnDelete(policy string) (string, error) {
	switch policy {
	case "cancel":
		return "JOB_STATE_CANCELLED", nil
	case "drain":
		return "JOB_STATE_DRAINING", nil
	default:
		return "", fmt.Errorf("Invalid `on_delete` policy: %s", policy)
	}
}

func validateAllowedStringValue(ss []string) schema.SchemaValidateFunc {
	return func(v interface{}, k string) (ws []string, errors []error) {
		value := v.(string)
		existed := false
		for _, s := range ss {
			if s == value {
				existed = true
				break
			}
		}
		if !existed {
			errors = append(errors, fmt.Errorf(
				"%q must contain a valid string value should in array %#v, got %q",
				k, ss, value))
		}
		return

	}
}
