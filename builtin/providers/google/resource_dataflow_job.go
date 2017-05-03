package google

import (
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

			"parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				ForceNew: true,
			},

			"project": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
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
	params := expandStringMap(d.Get("parameters").(map[string]interface{}))

	templateService := dataflow.NewProjectsTemplatesService(config.clientDataflow)

	env := dataflow.RuntimeEnvironment{
		TempLocation: tempLocation,
	}

	request := dataflow.CreateJobFromTemplateRequest{
		JobName:     jobName,
		GcsPath:     gcsPath,
		Parameters:  params,
		Environment: &env,
	}

	call := templateService.Create(project, &request)

	res, err := call.Do()
	if err != nil {
		return err
	}

	d.SetId(res.Id)

	return nil
}

func resourceDataflowJobRead(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	id := d.Id()

	call := config.clientDataflow.Projects.Jobs.Get(project, id)
	_, err = call.Do()
	if err != nil {
		return err
	}

	return nil
}

func resourceDataflowJobDelete(d *schema.ResourceData, meta interface{}) error {
	// TODO: Implement job cancellation.
	return nil
}

func expandStringMap(m map[string]interface{}) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		result[k] = v.(string)
	}
	return result
}
