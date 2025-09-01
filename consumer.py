import pika
import json
import fitz  
import base64
import os
from PIL import Image
from io import BytesIO
import datetime

RABBITMQ_HOST = "localhost"       
RABBITMQ_VHOST = "/"              
RABBITMQ_USER = "guest"           
RABBITMQ_PASS = "guest"           
QUEUE_NAME = "signix-docsign"      
ACK_QUEUE = "signix-docsign-ack" 

def base64_to_image(base64_string):
    """Convert base64 string to PIL Image"""
    try:
        image_data = base64.b64decode(base64_string)
        image = Image.open(BytesIO(image_data))
        return image
    except Exception as e:
        print(f"❌ Error converting base64 to image: {e}")
        return None


def find_text_in_pdf(doc, search_text):
    """Find text in PDF and return its coordinates"""
    found_instances = []
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        text_instances = page.search_for(search_text)
        
        for inst in text_instances:
            found_instances.append({
                'page': page_num,
                'rect': inst,
                'text': search_text
            })
            
    return found_instances


def apply_signatures_to_pdf(pdf_path, signed_pdf_path, signers, sign_data, doc_tags, font_coords=None):
    """Apply signatures to PDF document by finding tags directly in PDF"""
    try:
        # Open the PDF document
        doc = fitz.open(pdf_path)
        
        # Track applied signatures to avoid duplicates
        applied_signatures = set()
        
        # Process each doc tag and replace with its value directly
        for tag, value in doc_tags.items():
            print(f"🔍 Processing tag: {tag} = {value}")
            
            # Find all instances of the tag in the PDF
            tag_instances = find_text_in_pdf(doc, tag)
            
            if not tag_instances:
                print(f"⚠️ Tag '{tag}' not found in PDF")
                continue
            
            print(f"📍 Found {len(tag_instances)} instance(s) of tag {tag}")
            
            if tag not in applied_signatures:
                # Process ALL instances of the tag found
                for i, tag_instance in enumerate(tag_instances):
                    page_num = tag_instance['page']
                    tag_rect = tag_instance['rect']
                    
                    # Get the page
                    page = doc[page_num]
                    
                    # Replace the tag with its value (text)
                    # First, create a white rectangle to cover the existing tag
                    page.draw_rect(tag_rect, color=(1, 1, 1), fill=(1, 1, 1))
                    
                    # Then insert the new text value at the same location
                    page.insert_text(
                        (tag_rect.x0, tag_rect.y0 + tag_rect.height * 0.8),  # Adjust vertical position
                        value,
                        fontsize=10,
                        color=(0, 0, 0)  # Black text
                    )
                    
                    print(f"✅ Replaced tag {tag} instance {i+1}/{len(tag_instances)} with value '{value}' at page {page_num + 1}, coordinates ({tag_rect.x0:.1f}, {tag_rect.y0:.1f})")
                
                applied_signatures.add(tag)
        
        # Handle sign data (e.g., [[[Borr_sign]]]) - these are actual signature images
        for sign_tag, base64_data in sign_data.items():
            if base64_data and sign_tag not in applied_signatures:
                # Find all instances of the sign tag in the PDF
                tag_instances = find_text_in_pdf(doc, sign_tag)
                
                if tag_instances:
                    print(f"📍 Found {len(tag_instances)} instance(s) of signature tag {sign_tag}")
                    
                    sign_image = base64_to_image(base64_data)
                    if sign_image:
                        # Save signature as temporary image
                        temp_sig_path = f"temp_main_signature_{sign_tag.replace('[', '').replace(']', '').replace('_', '')}.png"
                        sign_image.save(temp_sig_path, "PNG")
                        
                        # Process ALL instances of the signature tag found
                        for i, tag_instance in enumerate(tag_instances):
                            page_num = tag_instance['page']
                            tag_rect = tag_instance['rect']
                            
                            # Get the page
                            page = doc[page_num]
                            
                            # Create signature rectangle
                            sig_width = max(tag_rect.width, 150)
                            sig_height = max(tag_rect.height, 40)
                            
                            # Position signature at the tag location
                            sig_x = tag_rect.x0
                            sig_y = tag_rect.y0
                            
                            signature_rect = fitz.Rect(sig_x, sig_y, sig_x + sig_width, sig_y + sig_height)
                            
                            # Insert signature image
                            page.insert_image(signature_rect, filename=temp_sig_path)
                            
                            print(f"✅ Applied main signature for {sign_tag} instance {i+1}/{len(tag_instances)} at page {page_num + 1}")
                        
                        applied_signatures.add(sign_tag)
                        
                        # Clean up temporary file
                        if os.path.exists(temp_sig_path):
                            os.remove(temp_sig_path)
                else:
                    print(f"⚠️ Sign tag '{sign_tag}' not found in PDF")
        
        # Handle signer signature images if they have base64SignData
        # for signer in signers:
        #     signer_name = signer.get('name', '')
        #     base64_sign_data = signer.get('base64SignData', '')
        #     signer_designation = signer.get('designation', '')
            
        #     if base64_sign_data:
        #         # Look for a tag that might represent this signer's signature location
        #         # This could be a separate signature tag or we might need to place it at a specific location
        #         signer_tag = f"[[[{signer_designation}_sign]]]"  # Example: [[[Notary_sign]]]
                
        #         tag_instances = find_text_in_pdf(doc, signer_tag)
                
        #         if tag_instances and signer_tag not in applied_signatures:
        #             print(f"📍 Found {len(tag_instances)} instance(s) of signer tag {signer_tag}")
                    
        #             sign_image = base64_to_image(base64_sign_data)
        #             if sign_image:
        #                 # Save signature as temporary image
        #                 temp_sig_path = f"temp_signature_{signer_name.replace(' ', '_')}.png"
        #                 sign_image.save(temp_sig_path, "PNG")
                        
        #                 # Process ALL instances of the signer tag found
        #                 for i, tag_instance in enumerate(tag_instances):
        #                     page_num = tag_instance['page']
        #                     tag_rect = tag_instance['rect']
                            
        #                     # Get the page
        #                     page = doc[page_num]
                            
        #                     # Create signature rectangle
        #                     sig_width = max(tag_rect.width, 150)
        #                     sig_height = max(tag_rect.height, 40)
                            
        #                     signature_rect = fitz.Rect(tag_rect.x0, tag_rect.y0, tag_rect.x0 + sig_width, tag_rect.y0 + sig_height)
                            
        #                     # Insert signature image
        #                     page.insert_image(signature_rect, filename=temp_sig_path)
                            
        #                     print(f"✅ Applied signature for {signer_name} ({signer_designation}) instance {i+1}/{len(tag_instances)} at page {page_num + 1}")
                        
        #                 applied_signatures.add(signer_tag)
                        
        #                 # Clean up temporary file
        #                 if os.path.exists(temp_sig_path):
        #                     os.remove(temp_sig_path)
        
        # Ensure signed directory exists
        signed_dir = os.path.dirname(signed_pdf_path)
        if signed_dir and not os.path.exists(signed_dir):
            os.makedirs(signed_dir, exist_ok=True)
        
        # Save the signed PDF
        doc.save(signed_pdf_path)
        doc.close()
        
        print(f"✅ Signed PDF saved: {signed_pdf_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error applying signatures to PDF: {e}")
        return False


def process_document_signing(message):
    """Process the document signing message"""
    try:
        signing_room_id = message.get('signingRoomId')
        original_path = message.get('originalPath', '')
        signed_path = message.get('signedPath', '')
        sign_data = message.get('signData', {})
        signers = message.get('signers', [])
        signed_documents = message.get('signedDocuments', [])
        
        print(f"🔄 Processing signing room ID: {signing_room_id}")
        
        # If signed_path is empty, use original_path with "signed_" prefix
        if not signed_path:
            signed_path = original_path
        
        processed_documents = []
        
        # Process each document
        for document in signed_documents:
            doc_name = document.get('name')
            doc_tags = document.get('docTags', {})
            
            # Construct file paths
            original_file_path = os.path.join(original_path.lstrip('/'), doc_name)
            signed_file_path = os.path.join(signed_path.lstrip('/'), f"signed_{doc_name}")
            
            print(f"📄 Processing document: {doc_name}")
            print(f"📂 Original path: {original_file_path}")
            print(f"📂 Signed path: {signed_file_path}")
            
            # Check if original file exists
            if not os.path.exists(original_file_path):
                print(f"❌ Original file not found: {original_file_path}")
                continue
            
            # Apply signatures to PDF (no longer needs font_coords parameter)
            success = apply_signatures_to_pdf(
                original_file_path, 
                signed_file_path, 
                signers, 
                sign_data, 
                doc_tags
            )
            
            if success:
                processed_documents.append({
                    'name': doc_name,
                    'original_path': original_file_path,
                    'signed_path': signed_file_path,
                    'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
                    'status': 'completed'
                })
            else:
                processed_documents.append({
                    'name': doc_name,
                    'original_path': original_file_path,
                    'signed_path': signed_file_path,
                    'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
                    'status': 'failed'
                })
        
        return {
            'signingRoomId': signing_room_id,
            'processedDocuments': processed_documents,
            'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
            'status': 'completed' if all(doc['status'] == 'completed' for doc in processed_documents) else 'partial'
        }
        
    except Exception as e:
        print(f"❌ Error processing document signing: {e}")
        return {
            'signingRoomId': message.get('signingRoomId'),
            'processedDocuments': [],
            'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
            'status': 'failed'
        }


def send_acknowledgment(channel, result):
    """Send acknowledgment back to producer"""
    try:
        # Send only the required data format
        ack_message = {
            'signingRoomId': result.get('signingRoomId'),
            'processedDocuments': result.get('processedDocuments', []),
            'timestamp': result.get('timestamp'),
            'status': result.get('status')
        }
        channel.queue_declare(queue=ACK_QUEUE, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=ACK_QUEUE,
            body=json.dumps(ack_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
    except Exception as e:
        print(f"❌ Error sending acknowledgment: {e}")


def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode("utf-8"))
        print(f"✅ Received message for signing room: {message.get('signingRoomId')}")
        
        # Process the document signing (now without JSON coordinate dependency)
        result = process_document_signing(message)
        
        # Send acknowledgment
        send_acknowledgment(ch, result)
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"❌ Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials
        )
    )

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"🚀 Waiting for messages from queue '{QUEUE_NAME}'. Press CTRL+C to exit.")
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("🛑 Stopping consumer...")
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == "__main__":
    main()
