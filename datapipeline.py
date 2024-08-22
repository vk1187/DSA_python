# Import necessary libraries
from flask import Flask, request, jsonify
import pyodbc
import json
from datetime import datetime
 
# Create a Flask application
app = Flask(__name__)
 
# SQL Server connection configuration
server = 'EVS01LAP6468'
database = 'test_chat'
connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Integrated Security=True;'


def get_db_connection():
    conn = pyodbc.connect(connection_string)
    return conn
 
@app.route('/', methods=['GET'])
def index():
    return 'Welcome to the ajay!'
 
# Route to add a new chat history
@app.route('/add_chat/', methods=['post'])
def add_chat():
    data = request.get_json()
    print("data", data)
    
    username = data['username']
    client_id = data['client_id']
    project_id = data['project_id']
    product_id = data['product_id']
    chat_json = json.dumps(data['chat_JSON'])  # Convert array of objects to JSON string
    conversation_id = data['conversation_id']
    created_date = datetime.now()
    updated_date = datetime.now()
    is_active = True
    
    conn = get_db_connection()
    cursor = conn.cursor()
    print("cursor", cursor)
    
    cursor.execute("""
        INSERT INTO chat_data (username, client_id, project_id, product_id, chat_JSON, conversation_id, created_date, updated_date, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (username, client_id, project_id, product_id, chat_json, conversation_id, created_date, updated_date, is_active))
    
    conn.commit()
    conn.close()
 
    return jsonify({'message': 'Chat history added successfully'}), 201
 
# Route to get a chat history by ID
@app.route('/get_chat_by_id/<int:id>', methods=['GET'])
def get_chat_by_id(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM chat_data WHERE id =?", (id,))
    row = cursor.fetchone()
    conn.close()
 
    if row:
        chat_data = {
            'id': row.id,
            'username': row.username,
            'client_id': row.client_id,
            'project_id': row.project_id,
            'product_id': row.product_id,
            'chat_JSON': json.loads(row.chat_JSON),  # Convert JSON string back to array of objects
            'conversation_id': row.conversation_id,
            'created_date': row.created_date,
            'updated_date': row.updated_date,
            'is_active': row.is_active
        }
        return jsonify(chat_data), 200
    else:
        return jsonify({'message': 'Chat history not found'}), 404
 
# Route to get a chat history by conversation ID
@app.route('/get_chat_by_conversationid/<conversationid>', methods=['GET'])
def get_chat_by_conversationid(conversationid):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM chat_History WHERE Conversation_id =?", (conversationid,))
    row = cursor.fetchone()
    conn.close()
 
    if row:
        chat_data = {
            'id': row.id,
            'username': row.Username,
            'client_id': row.ClientID,
            'project_id': row.ProjectID,
            'product_id': row.ProductID,
            'chat_JSON': json.loads(row.ChatJson),  # Convert JSON string back to array of objects
            'conversation_id': row.ConversationID,
            'created_date': row.CreatedDate,
            'updated_date': row.UpdatedDate,
            'is_active': row.IsActive
        }
        return jsonify(chat_data), 200
    else:
        return jsonify({'message': 'Chat history not found'}), 404
 
# Route to update a chat history
@app.route('/update_chat/<int:id>', methods=['PUT'])
def update_chat(id):
    data = request.get_json()
    chat_json = json.dumps(data['chat_JSON'])  
    updated_date = datetime.now()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE chat_History
        SET Chat_JSON =?, Updated_date =?
        WHERE id =?
    """, (chat_json, updated_date, id))
    conn.commit()
    conn.close()
 
    return jsonify({'message': 'Chat history updated successfully'}), 200
 
# Route to delete a chat history
@app.route('/delete_chat/<int:id>', methods=['DELETE'])
def delete_chat(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM chat_History WHERE id =?", (id,))
    conn.commit()
    conn.close()
 
    return jsonify({'message': 'Chat history deleted successfully'}), 200
 
if __name__ == '__main__':
    app.run(host='localhost', port=8080, debug=True)