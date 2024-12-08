from libs.analysis_functions import (
    initialize_spark, load_data, preprocess_movies_ratings,
    calculate_avg_ratings_by_genre, calculate_ratings_by_year,
    inspect_data, save_to_csv, merge_datasets
)
import matplotlib.pyplot as plt

# Initialize Spark
spark = initialize_spark()

# Define dataset path
path = "/Users/simonemantero/Desktop/hw4/dataset"

# Load Data
tag_df, rating_df, movie_df, link_df, genome_scores_df, genome_tags_df = load_data(spark, path)



# Inspect the structure and content of the loaded DataFrames
print("Inspecting rating_df:")
inspect_data(rating_df, num_rows=10)  # Display top 10 rows of ratings

print("Inspecting movie_df:")
inspect_data(movie_df, num_rows=5)  # Display top 5 rows of movies

# Convert to Pandas for detailed inspection
movies_sample = inspect_data(movie_df, num_rows=5, convert_to_pandas=True)
print(movies_sample)



# Preprocess movies and ratings data
movies_ratings_df = preprocess_movies_ratings(movie_df, rating_df)

# Inspect preprocessed data
print("Inspecting movies_ratings_df after preprocessing:")
inspect_data(movies_ratings_df, num_rows=5)

# Calculate average ratings by genre
avg_ratings_by_genre = calculate_avg_ratings_by_genre(movies_ratings_df)

# Calculate ratings by year
ratings_by_year = calculate_ratings_by_year(movies_ratings_df)




import libs
from libs.analysis_functions import merge_datasets


# Import necessary modules
from libs.analysis_functions import initialize_spark, load_data, merge_datasets, inspect_data

# Initialize Spark session
spark = initialize_spark()

# Load datasets
path = "/Users/roberto/Desktop/HM4-ADM/datasets"  # Update with the actual path to your dataset
tag_df, rating_df, movie_df, link_df, genome_scores_df, genome_tags_df = load_data(spark, path)

# Merge datasets into a single DataFrame
final_dataset = merge_datasets(tag_df, rating_df, movie_df, link_df, genome_scores_df, genome_tags_df)

# Visualize the merged dataset
print("Dataset Schema:")
final_dataset.printSchema()  # Display the schema of the DataFrame

print("\nSample Rows:")
final_dataset.show(10)  # Display the first 10 rows of the dataset



# Display dataset schema and sample rows for a better understanding of columns and structure

# Tag Dataset
print("Tag Dataset Schema:")
tag_df.printSchema()
print("\nSample Rows from Tag Dataset:")
tag_df.show(5)

# Rating Dataset
print("\nRating Dataset Schema:")
rating_df.printSchema()
print("\nSample Rows from Rating Dataset:")
rating_df.show(5)

# Movie Dataset
print("\nMovie Dataset Schema:")
movie_df.printSchema()
print("\nSample Rows from Movie Dataset:")
movie_df.show(5)

# Link Dataset
print("\nLink Dataset Schema:")
link_df.printSchema()
print("\nSample Rows from Link Dataset:")
link_df.show(5)

# Genome Scores Dataset
print("\nGenome Scores Dataset Schema:")
genome_scores_df.printSchema()
print("\nSample Rows from Genome Scores Dataset:")
genome_scores_df.show(5)

# Genome Tags Dataset
print("\nGenome Tags Dataset Schema:")
genome_tags_df.printSchema()
print("\nSample Rows from Genome Tags Dataset:")
genome_tags_df.show(5)

# Merged Dataset
print("\nMerged Dataset Schema:")
final_dataset.printSchema()
print("\nSample Rows from Merged Dataset:")
final_dataset.show(10)

# Display Summary Statistics
print("\nSummary Statistics for Merged Dataset:")
final_dataset.describe().show()

# Column Analysis
print("\nColumn Names and Data Types in the Merged Dataset:")
for col_name, dtype in final_dataset.dtypes:
    print(f"{col_name}: {dtype}")


print("Merged Dataset Schema:")
final_dataset.printSchema()




# Schema and sample rows for each dataset
datasets = {"Tag Dataset": tag_df, "Rating Dataset": rating_df, "Movie Dataset": movie_df,
            "Link Dataset": link_df, "Genome Scores Dataset": genome_scores_df, "Genome Tags Dataset": genome_tags_df}

for name, dataset in datasets.items():
    print(f"\n{name} Schema:")
    dataset.printSchema()
    print(f"\nSample Rows from {name}:")
    dataset.show(5)



tag_df = tag_df.withColumnRenamed("tag", "user_tag")
genome_tags_df = genome_tags_df.withColumnRenamed("tag", "genome_tag")


from pyspark.sql.functions import col  # Add this import to resolve the NameError

# Ensure movieId and tagId columns are cast to integer type
tag_df = tag_df.withColumn("movieId", col("movieId").cast("integer"))
genome_scores_df = genome_scores_df.withColumn("movieId", col("movieId").cast("integer"))
genome_tags_df = genome_tags_df.withColumn("tagId", col("tagId").cast("integer"))


user_movie_df = movie_df.join(rating_df, "movieId", "inner")
user_movie_df = user_movie_df.join(tag_df, ["userId", "movieId"], "left")
genome_df = genome_scores_df.join(genome_tags_df, "tagId", "inner")



final_dataset = merge_datasets(tag_df, rating_df, movie_df, link_df, genome_scores_df, genome_tags_df)

print("Columns in Final Dataset:")
final_dataset.printSchema()


final_dataset = final_dataset.fillna({
    "user_tag": "No Tag",
    "genome_tag": "No Tag",
    "genres": "Unknown",
    "relevance": 0.0
})



print("Final Dataset Schema:")
final_dataset.printSchema()


# View the first few rows to confirm the data
print("Sample Rows from Final Dataset:")
final_dataset.show(10)

# Describe numeric columns
print("\nSummary Statistics for Numeric Columns:")
final_dataset.describe(["rating", "relevance"]).show()

# Check for missing values
print("\nCount of Null Values in Each Column:")
final_dataset.select([col(c).isNull().alias(c) for c in final_dataset.columns]).agg(
    *(count(c).alias(c) for c in final_dataset.columns)
).show()



# Count ratings by value
print("Ratings Distribution:")
final_dataset.groupBy("rating").count().orderBy("rating").show()



# Convert Spark DataFrames to Pandas for visualization
avg_ratings_by_genre_pd = avg_ratings_by_genre.toPandas()
ratings_by_year_pd = ratings_by_year.toPandas()

# Plot average ratings by genre
plt.figure(figsize=(10, 6))
avg_ratings_by_genre_pd.plot(kind="bar", x="genre", y="average_rating", legend=False)
plt.title("Average Ratings by Genre")
plt.xlabel("Genre")
plt.ylabel("Average Rating")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot ratings by year
plt.figure(figsize=(10, 6))
plt.plot(ratings_by_year_pd["year"], ratings_by_year_pd["average_rating"], marker="o")
plt.title("Average Ratings by Year")
plt.xlabel("Year")
plt.ylabel("Average Rating")
plt.grid()
plt.tight_layout()
plt.show()


# Preprocess movies and ratings data
movies_ratings_df = preprocess_movies_ratings(movie_df, rating_df)

# Calculate average ratings by genre
avg_ratings_by_genre = calculate_avg_ratings_by_genre(movies_ratings_df)

# Calculate ratings by year
ratings_by_year = calculate_ratings_by_year(movies_ratings_df)

# Save results for external use
save_to_csv(avg_ratings_by_genre, f"{path}/avg_ratings_by_genre.csv")
save_to_csv(ratings_by_year, f"{path}/ratings_by_year.csv")




# Convert Spark DataFrames to Pandas for visualization
avg_ratings_by_genre_pd = avg_ratings_by_genre.toPandas()
ratings_by_year_pd = ratings_by_year.toPandas()

# Plot average ratings by genre
plt.figure(figsize=(10, 6))
avg_ratings_by_genre_pd.plot(kind="bar", x="genre", y="average_rating", legend=False)
plt.title("Average Ratings by Genre")
plt.xlabel("Genre")
plt.ylabel("Average Rating")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot ratings by year
plt.figure(figsize=(10, 6))
plt.plot(ratings_by_year_pd["year"], ratings_by_year_pd["average_rating"], marker="o")
plt.title("Average Ratings by Year")
plt.xlabel("Year")
plt.ylabel("Average Rating")
plt.grid()
plt.tight_layout()
plt.show()