from pyspark.sql import SparkSession

# Create a spark session
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

# Use sql() to write a raw SQL query
df = spark.sql("SELECT 'Hello World' as hello")

# Print the dataframe
df.show()
df.write.mode("overwrite").json("results")
    - run: spark-submit --version
    - run: spark-submit --master local hello.py
    - run: ls -la  
    runs-on: ubuntu-latest

    permissions:
      # Give the default GITHUB_TOKEN write permission to commit and push the
      # added or changed files to the repository.
      contents: write
    - name: GIT commit and push docs
      env: 
        CI_COMMIT_MESSAGE: save spark results
        CI_COMMIT_AUTHOR: adsoft 
      run: |
        git config --global user.name "${{ env.CI_COMMIT_AUTHOR }}"
        git config --global user.email "adsoft@live.com.mx"
        git add results
        git commit -m "${{ env.CI_COMMIT_MESSAGE }}"
        git push
