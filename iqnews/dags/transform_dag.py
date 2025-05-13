from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import re
import torch
from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM

# Default arguments untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,  # Menambah jumlah retry
    'retry_delay': timedelta(minutes=2),
}

# Definisi DAG
dag = DAG(
    'market_transform',
    default_args=default_args,
    description='Membuat transformasi dalam bentuk ringkasan',
    schedule='*/30 * * * *',  # Berjalan setiap hari pukul 7 pagi
    catchup=False,
    max_active_runs=5,  # Jumlah maksimum instance DAG yang dapat berjalan secara bersamaan
    max_active_tasks=3,  # Jumlah task yang dapat berjalan secara bersamaan dalam satu DAG
)

# Fungsi untuk inisialisasi model (dijalankan hanya sekali per worker)
def initialize_model(**context):
    print("🚀 Memulai inisialisasi model summarization...")
    model_name = "facebook/bart-large-cnn"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
    device = 0 if torch.cuda.is_available() else -1
    summarizer = pipeline("summarization", model=model, tokenizer=tokenizer, device=device)
    
    # Simpan model ke XCom untuk digunakan oleh task lain
    context['ti'].xcom_push(key='model_initialized', value=True)
    print("✅ Model berhasil diinisialisasi")
    
    # Return objek model dan tokenizer sebagai referensi global
    return {
        'model': model,
        'tokenizer': tokenizer,
        'summarizer': summarizer
    }

# Variable global untuk menyimpan model
MODEL_OBJECTS = None

# Fungsi untuk membersihkan teks
def clean_text(text):
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"IQPlus,|\"|"|"|'|'|\(end\)", "", text)
    return text.strip()

# Fungsi membagi teks menjadi chunk berdasarkan karakter
def chunk_text_by_char(text, max_chars=1000):
    sentences = text.split('. ')
    chunks = []
    current_chunk = ""

    for sentence in sentences:
        if len(current_chunk) + len(sentence) < max_chars:
            current_chunk += sentence + ". "
        else:
            chunks.append(current_chunk.strip())
            current_chunk = sentence + ". "
    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks

# Fungsi untuk meringkas teks
def summarize_text(text, summarizer):
    cleaned = clean_text(text)

    if len(cleaned) < 30:
        print("⚠️ Teks terlalu pendek, tidak diringkas.")
        return cleaned

    chunks = chunk_text_by_char(cleaned)
    summaries = []

    for chunk in chunks:
        try:
            summary = summarizer(chunk, max_length=250, min_length=50, do_sample=False)[0]['summary_text']
            summaries.append(summary)
        except Exception as e:
            print(f"⚠️ Error saat meringkas chunk: {e}")
            continue

    return " ".join(summaries)

# Fungsi untuk menjalankan batch processing
def process_batch(batch_size=10, **context):
    global MODEL_OBJECTS
    
    # Ambil model dari task sebelumnya jika MODEL_OBJECTS masih None
    if MODEL_OBJECTS is None:
        # Inisialisasi model
        model_name = "facebook/bart-large-cnn"
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        device = 0 if torch.cuda.is_available() else -1
        summarizer = pipeline("summarization", model=model, tokenizer=tokenizer, device=device)
        MODEL_OBJECTS = {
            'model': model,
            'tokenizer': tokenizer,
            'summarizer': summarizer
        }
    
    # Buat koneksi ke MongoDB menggunakan MongoHook
    mongo_hook = MongoHook(conn_id='mongo_conn')
    client = mongo_hook.get_conn()
    db = client["news_db"]
    collection = db["articles2"]
    
    # Ambil last_id dari XCom jika ada
    task_instance = context['ti']
    last_id = task_instance.xcom_pull(key='last_processed_id', include_prior_dates=True)
    
    print(f"🔍 Memulai batch processing dengan batch_size={batch_size}, last_id={last_id}")
    
    # Ambil batch artikel
    query = {"_id": {"$gt": last_id}} if last_id else {}
    # Tambahkan filter untuk dokumen yang belum memiliki ringkasan
    query.update({"$or": [{"ringkasan": {"$exists": False}}, {"ringkasan": None}, {"ringkasan": ""}]})
    
    batch = list(collection.find(query).sort("_id", 1).limit(batch_size))
    
    if not batch:
        print("✅ Semua artikel sudah diproses.")
        return 0
    
    processed_count = 0
    for doc in batch:
        doc_id = doc["_id"]
        original_text = doc.get("konten", "")
        last_id = doc_id  # update untuk batch selanjutnya
        
        if not original_text or not original_text.strip():
            collection.update_one({"_id": doc_id}, {"$set": {"ringkasan": None}})
            print(f"⚠️ Kosong/null pada ID: {doc_id}, disimpan None.")
            continue
        
        print(f"\n📄 Memproses ID: {doc_id}")
        summary = summarize_text(original_text, MODEL_OBJECTS['summarizer'])
        collection.update_one({"_id": doc_id}, {"$set": {"ringkasan": summary}})
        processed_count += 1
        
        if summary:
            print(f"✅ Ringkasan disimpan untuk ID: {doc_id}")
        else:
            print(f"⚠️ Ringkasan gagal, disimpan None.")
    
    # Simpan last_id ke XCom untuk digunakan di run berikutnya
    task_instance.xcom_push(key='last_processed_id', value=last_id)
    
    return processed_count

# Fungsi untuk menjalankan proses lengkap transformasi data
def process_all_articles(**context):
    batch_size = 10
    total_processed = 0
    while True:
        processed = process_batch(batch_size=batch_size, **context)
        if processed == 0:
            break
        total_processed += processed
        print(f"🔄 Total artikel yang telah diproses: {total_processed}")
    
    return total_processed

# Define tasks
init_model_task = PythonOperator(
    task_id='initialize_model',
    python_callable=initialize_model,
    provide_context=True,
    dag=dag,
)

process_articles_task = PythonOperator(
    task_id='process_articles',
    python_callable=process_all_articles,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
init_model_task >> process_articles_task