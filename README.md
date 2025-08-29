This repo hosts the code, dockerfile, and requirements.txt file for the 'process_url' function in google cloud run. This repo is connected to the function via cloud build. Because of this, any changes made to these files will automatically redeploy the function, so please be cautious when making edits.

### Files Contained
1. Main.py
     - This is the file with the actual code for the functions. It includes:
         - 2 generic pdf download functions, one with aiohttp and one with playwright/selenium fallbacks.
         - Publisher specific download functions if the universal ones fail.
         - Using pdfplumber to extract text from the downloaded file.
         - Uploading the pdf and text file to GCS.
         - Appending metadata for the fields 'openAccessStatus','pdfPublicLink','textPublicLink', and 'pdfSource' in Firestore.
2. Dockerfile
     - Builds a custom cloudrun container that includes Playwright and it's requirements, which were not available in the default cloud run. In initial testing when I switched from my local environment to cloud run, playwright was not installing properly and all my download functions were failing, so this docker container is the solution to that issue.
3. requirements.txt
     - Lists all dependencies required for this code to execute properly and ensures the same version of libraries are used everytime.

### Using the cloud run function
This function takes a dataframe as the input, and requires that the dataframe as the columns " ".
The function will download the pdf at the urls inputted, trying the OpenAlex urls first and then the Semantic Scholar urls if OA fails.
It will then keep a record of what publisher the pdf came from, use pdfplumber to create a text file for the document, upload the pdf and the text file to the Google Cloud Storage bucket "open-access-publications", and append the appropriate metadata to firestore.
I have been using the following code in visual studio to use this function, where results.df is my dataframe of outputs from the API calls and I chose a batch of 500 so I was frequently saving in case of timeouts:

```
batch_size = 500
num_batches = math.ceil(len(results_df) / batch_size)

batches = [results_df.iloc[i*batch_size:(i+1)*batch_size] for i in range(num_batches)]
def send_batch(batch_df, function_url):
    payload = json.dumps({"data": batch_df.to_dict(orient='records')})
    token = subprocess.check_output(
        ["gcloud", "auth", "print-identity-token"], text=True
    ).strip()

    result = subprocess.run(
        [
            "curl",
            "-X", "POST",
            function_url,
            "-H", f"Authorization: Bearer {token}",
            "-H", "Content-Type: application/json",
            "-d", payload
        ],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print("Error:", result.stderr)
        return None
    return result.stdout

FUNCTION_URL = "https://process-url-173921976155.us-central1.run.app"
output_dir = "/Users/rallan/Desktop/Paper Pullin'"
os.makedirs(output_dir, exist_ok=True)

for i, batch in enumerate(batches):
    print(f"Processing batch {i+1}/{num_batches}...")
    batch_result = send_batch(batch, FUNCTION_URL)
    
    if batch_result is not None:
        with open(f"{output_dir}/batch_{i+1}.json", "w") as f:
            f.write(batch_result)

```
