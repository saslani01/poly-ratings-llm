#!/usr/bin/env python3
"""
Load processed review documents into ChromaDB vector store
Usage: python -m data_pipeline.load_to_chroma [--reset] [--stats]
    --reset: Deletes existing db
    --stats: Gets the last stat of loading
"""

import json
from pathlib import Path
from datetime import datetime
import argparse
import chromadb
from chromadb.config import Settings

class VectorStorePrep:
    def __init__(self):
        self.processed_dir = Path("data/processed")
        self.vector_db_dir = Path("data/vector_db")
        self.vector_db_dir.mkdir(parents=True, exist_ok=True)
        
        # ChromaDB setup
        self.client = chromadb.PersistentClient(
            path=str(self.vector_db_dir)
        )
        self.collection_name = "professor_reviews"
    
    def log_message(self, message, level="INFO"):
        """Simple logging"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")
    
    def reset_collection(self):
        """Delete existing collection and create new one"""
        try:
            # Delete existing collection if it exists
            try:
                self.client.delete_collection(self.collection_name)
                self.log_message("Deleted existing collection")
            except Exception:
                pass  # Collection doesn't exist, that's fine
            
            # Create new collection
            collection = self.client.create_collection(
                name=self.collection_name,
                metadata={"description": "Cal Poly professor reviews for chatbot"}
            )
            self.log_message("Created new collection")
            return collection
            
        except Exception as e:
            self.log_message(f"Error resetting collection: {e}", "ERROR")
            return None
    
    def get_or_create_collection(self):
        """Get existing collection or create new one"""
        try:
            # Try to get existing collection
            collection = self.client.get_collection(self.collection_name)
            self.log_message("Using existing collection")
            return collection
        except Exception:
            # Collection doesn't exist, create it
            try:
                collection = self.client.create_collection(
                    name=self.collection_name,
                    metadata={"description": "Cal Poly professor reviews for chatbot"}
                )
                self.log_message("Created new collection")
                return collection
            except Exception as e:
                self.log_message(f"Error creating collection: {e}", "ERROR")
                return None
    
    def load_processed_reviews(self):
        """Load processed reviews from JSON file"""
        reviews_file = self.processed_dir / "reviews.json"
        
        if not reviews_file.exists():
            self.log_message("No processed reviews found. Run processing first.", "ERROR")
            return None
        
        try:
            with open(reviews_file, 'r', encoding='utf-8') as f:
                reviews = json.load(f)
            
            self.log_message(f"Loaded {len(reviews)} processed reviews")
            return reviews
            
        except Exception as e:
            self.log_message(f"Error loading reviews: {e}", "ERROR")
            return None
    
    def prepare_for_chromadb(self, reviews):
        """Prepare review data for ChromaDB format"""
        documents = []
        metadatas = []
        ids = []
        
        for i, review in enumerate(reviews):
            # Extract document text
            document = review.get('document', '').strip()
            if not document:
                continue
            
            # Extract metadata
            metadata = review.get('metadata', {})
            
            # Clean metadata (ChromaDB doesn't like None values)
            clean_metadata = {}
            for key, value in metadata.items():
                if value is not None:
                    clean_metadata[key] = str(value) if not isinstance(value, (str, int, float, bool)) else value
            
            # Create unique ID
            review_id = metadata.get('review_id')
            if not review_id:
                review_id = f"review_{i}"
            
            documents.append(document)
            metadatas.append(clean_metadata)
            ids.append(str(review_id))
        
        self.log_message(f"Prepared {len(documents)} documents for ChromaDB")
        return documents, metadatas, ids
    
    def load_into_chromadb(self, reset=False):
        """Load reviews into ChromaDB collection"""
        self.log_message("Starting ChromaDB loading...")
        
        # Load processed reviews
        reviews = self.load_processed_reviews()
        if not reviews:
            return False
        
        # Get or create collection
        if reset:
            collection = self.reset_collection()
        else:
            collection = self.get_or_create_collection()
        
        if not collection:
            return False
        
        # Check if collection already has data
        if not reset:
            try:
                existing_count = collection.count()
                if existing_count > 0:
                    self.log_message(f"Collection already has {existing_count} documents", "WARN")
                    response = input("Reset collection? (y/N): ").strip().lower()
                    if response == 'y':
                        collection = self.reset_collection()
                        if not collection:
                            return False
                    else:
                        self.log_message("Skipping load to avoid duplicates")
                        return True
            except Exception:
                pass  # Continue with loading
        
        # Prepare data for ChromaDB
        documents, metadatas, ids = self.prepare_for_chromadb(reviews)
        
        if not documents:
            self.log_message("No valid documents to load", "ERROR")
            return False
        
        # Load in batches to avoid memory issues
        batch_size = 1000
        total_batches = (len(documents) + batch_size - 1) // batch_size
        
        try:
            for i in range(0, len(documents), batch_size):
                batch_docs = documents[i:i + batch_size]
                batch_metas = metadatas[i:i + batch_size]
                batch_ids = ids[i:i + batch_size]
                
                batch_num = (i // batch_size) + 1
                self.log_message(f"Loading batch {batch_num}/{total_batches} ({len(batch_docs)} documents)")
                
                collection.add(
                    documents=batch_docs,
                    metadatas=batch_metas,
                    ids=batch_ids
                )
            
            # Verify final count
            final_count = collection.count()
            
            # Save loading stats
            stats = {
                'timestamp': datetime.now().isoformat(),
                'documents_loaded': final_count,
                'collection_name': self.collection_name,
                'vector_db_path': str(self.vector_db_dir)
            }
            
            stats_file = self.processed_dir / "vector_db_stats.json"
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2)
            
            self.log_message(f"Successfully loaded {final_count} documents into ChromaDB")
            self.log_message(f"Vector database saved to: {self.vector_db_dir}")
            
            return True
            
        except Exception as e:
            self.log_message(f"Error loading into ChromaDB: {e}", "ERROR")
            return False
    
    def show_stats(self):
        """Show vector database statistics"""
        # Check if vector DB exists
        stats_file = self.processed_dir / "vector_db_stats.json"
        
        if stats_file.exists():
            with open(stats_file, 'r') as f:
                stats = json.load(f)
            print(f"Last vector DB load: {stats['timestamp']}")
            print(f"Documents in vector DB: {stats['documents_loaded']}")
            print(f"Collection name: {stats['collection_name']}")
            print(f"Vector DB path: {stats['vector_db_path']}")
        else:
            print("No vector database statistics found.")
        
        # Try to get current collection stats
        try:
            collection = self.client.get_collection(self.collection_name)
            current_count = collection.count()
            print(f"Current document count: {current_count}")
        except Exception:
            print("No active collection found.")

def main():
    """CLI interface for vector store prep"""
    parser = argparse.ArgumentParser(
        description="Load processed reviews into ChromaDB vector store"
    )
    
    parser.add_argument('--reset', action='store_true',
                       help='Reset collection (delete existing data)')
    parser.add_argument('--stats', action='store_true',
                       help='Show vector database statistics')
    
    args = parser.parse_args()
    
    prep = VectorStorePrep()
    
    if args.stats:
        prep.show_stats()
    else:
        success = prep.load_into_chromadb(reset=args.reset)
        if success:
            print("\n✓ Vector database preparation completed successfully!")
            print("Next step: Build the chatbot interface")
        else:
            print("\n✗ Vector database preparation failed. Check the logs above.")

if __name__ == "__main__":
    main()