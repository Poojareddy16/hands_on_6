# 🎵 Music Streaming Analysis Using Spark Structured APIs

## 📌 Overview
This project analyzes **user listening behavior** and **music trends** using the **Spark Structured API**.  
The objective is to process structured data from a fictional music streaming platform to gain insights into:
- Genre preferences  
- Song popularity  
- Listener engagement patterns  

The assignment is part of **ITCS 6190/8190 — Cloud Computing for Data Analysis, Fall 2025** :contentReference[oaicite:1]{index=1}.

---

## 📂 Dataset Description
Two CSV files are generated with the provided input generator:
1. **listening_logs.csv**  
   - `user_id`: Unique ID of the user  
   - `song_id`: Unique ID of the song  
   - `timestamp`: Date and time the song was played (e.g., 2025-03-23 14:05:00)  
   - `duration_sec`: Duration in seconds for which the song was played  

2. **songs_metadata.csv**  
   - `song_id`: Unique ID of the song  
   - `title`: Title of the song  
   - `artist`: Name of the artist  
   - `genre`: Genre of the song (Pop, Rock, Jazz, etc.)  
   - `mood`: Mood category (Happy, Sad, Energetic, Chill, etc.)  

---

## Repository Structure
```bash
your_repo/
 ├── listening_logs.csv
 ├── songs_metadata.csv
 ├── main.py
 ├── README.md
 └── output/
     ├── user_favorite_genres/
     ├── avg_listen_time_per_song/
     ├── genre_loyalty_scores/
     └── night_owl_users/
```

---

## Output Directory Structure
As required by the assignment:contentReference[oaicite:0]{index=0}, each task’s output is saved into its own subfolder under `output/`:

```bash
output/
 ├── user_favorite_genres/       
 ├── avg_listen_time_per_song/   
 ├── genre_loyalty_scores/       
 └── night_owl_users/
```
---          

## Tasks and Outputs
### 🔹 Task 1: User Favorite Genres
- **Goal:** Identify each user’s most-listened genre.  
- **Method:**  
  1. Join listening logs with song metadata to get `genre`.  
  2. Count plays per `(user_id, genre)`.  
  3. Select the top genre for each user using a ranking window.  
- **Output Folder:** `output/user_favorite_genres/`  
- **Preview command:**
```bash
part-00000-2c13d93f-84ca-45d2-aba0-88f45f32e0a2-c000.csv
```

---

### 🔹 Task 2: Average Listen Time per Song
- **Goal:** Calculate the average listening duration (in seconds) for each song.  
- **Method:**  
  1. Group listening logs by `song_id`.  
  2. Compute the average of `duration_sec`.  
  3. Join with `songs_metadata.csv` to include the `title` of each song.  
- **Output Folder:** `output/avg_listen_time_per_song/`  
- **Preview command:**
```bash
part-00000-208690eb-dc6d-4dab-b074-590dda01ce91-c000.csv
```

---

### 🔹 Task 3: Genre Loyalty Scores
- **Goal:** Measure how loyal users are to their favorite genre.  

- **Method:**  
  1. Count **total plays** per user.  
  2. Find the **top genre count** for each user’s favorite genre.  
  3. Compute the loyalty score:  
     \[
     loyalty\_score = \frac{top\_genre\_count}{total\_plays}
     \]  
  4. Filter and keep only users with `loyalty_score > 0.8`.  

- **Output Folder:**  
  `output/genre_loyalty_scores/`  

- **Preview command:**
```bash
part-00000-7caa52af-cff6-4891-94fb-01a4eaa62a8b-c000.csv
```

---


### 🔹 Task 4: Night Owl Users
- **Goal:** Identify users who listen to music between **12 AM and 5 AM**.  

- **Method:**  
  1. Convert the `timestamp` field into an hour using Spark’s `hour()` function.  
  2. Filter listening logs where `0 ≤ hour < 5`.  
  3. Extract distinct `user_id`s of these listeners.  

- **Output Folder:**  
  `output/night_owl_users/`  

- **Preview command:**
```bash
part-00000-1a46b4c9-35fd-4af2-9b4c-bccdf72bcb73-c000.csv
```

---

## Execution Instructions
## ⚙️ Execution Instructions

### 1. Generate Input Data
Run the provided input generator to create the required CSVs (`listening_logs.csv` and `songs_metadata.csv`):

```bash
python3 input_generator.py
```

---
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 input_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

---
## 🛠️ Errors and Resolutions

During execution, you may encounter common issues. Below are fixes:
