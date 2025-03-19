### **Things I've learnt**

- Terraform is an infrastructure as a code framework. 
- Instead of manually navigating the Cloud UI and manually spinning up resources, terraform helps spin up resources simultaneously in the form of code/instructions and destroy them when not needed. Also supports version control.
- You will require a key generated from GCP's service account with the following permissions, `Storage Admin` & `Bigquery Admin`
- Navigate to the service account and click 'Manage keys'. Click on 'Add key' and generate and JSON file.
- Copy the contents of the key under './keys/my-creds.json'

### **Challenges faced**
-

## Commands (Terraform)
`terraform init`        [Initialize project directory as terraform project]  
`terraform plan`        [previews the plan dictating what resources would be spun up]  
`terraform apply`       [apply the created plan from main.tf]  
`terraform destroy`     [terminate actively running plans and destroy all created resources]  