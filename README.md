# **Pipeline Batch Bovespa - Tech Challenge 2**

<br/>
<p align="center">
  <a href="https://www.fiap.com.br/"><img src="https://upload.wikimedia.org/wikipedia/commons/d/d4/Fiap-logo-novo.jpg" width="300" alt="FIAP"></a>
</p>
<br>

## The Pipeline
A fully automated data pipeline was designed and implemented to extract, process, and analyze trading session data from B3 using AWS services such as S3, Glue, Lambda, and Athena. The pipeline operates in batch mode and consists of the following steps:

1. Trading data is collected from the B3 website using Python and Selenium WebDriver.
2. The raw data is uploaded to an S3 bucket, which triggers a Lambda function via a PUT event.
3. The Lambda function starts the Bovespa Batch job in AWS Glue.
4. The Glue job processes the data, performing transformations, partitioning, and loading it into another S3 bucket.
5. The processed data is made queryable through Athena, using tables registered in the Glue Data Catalog.


## Virtual Env

```bash
$ python3.12 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

## Running Local Scripts

- `$ python download.py` - Runs B3 scraping
- `$ python pipeline_util.py` - Runs simple data cleaning steps if needed


## Common Issues
- **Missing dependencies:** If any dependencies are missing, check requirements.txt to ensure everything is installed.
<br>

## **Developers**

<table border="0" align="center">
  <tr>
  <td align="center">
      <img src="https://avatars.githubusercontent.com/u/71346377?v=4" width="160px" alt="Foto do Alexandre"/><br>
      <sub>
        <a href="https://www.github.com/alexandre-tvrs">@Alexandre Tavares</a>
      </sub>
    </td>
        <td align="center">
      <img src="https://avatars.githubusercontent.com/u/160500127?v=4" width="160px" alt="Foto do Paulo"/><br>
      <sub>
        <a href="https://github.com/PauloMukai">@Paulo Mukai</a>
      </sub>
    </td>
    </td>
        <td align="center">
      <img src="https://avatars.githubusercontent.com/u/160500128?v=4" width="160px" alt="Foto da Vanessa"/><br>
      <sub>
        <a href="https://github.com/AnjosVanessa">@Anjos Vanessa</a>
      </sub>
    </td>
  </tr>
</table>