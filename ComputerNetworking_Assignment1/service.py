import os
from flask import Flask, request, jsonify
from peer import *

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

MYIP = '10.0.184.207'
peer = Peer(ip=MYIP, port=4041, pieces_storage="pieces", metainfo_storage="metainfo")

@app.route('/starting', methods=['GET'])
def start_file():
    try:
        peer.running=True
        peer.join_network()
        peer.run()
        return jsonify({'message': 'Peer starts receiving responses'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    filename_list = request.json.get('filename_list')
    if not filename_list:
        return jsonify({'error': 'No file names provided'}), 400
    try:
        list_up = filename_list
        peer.tit_for_tat += len(list_up)
        peer.upload_files(list_up, (peer.tracker_ip, peer.tracker_port))
        return jsonify({'message': 'File names processed for upload'}), 201
    except Exception as e:
        return jsonify({'error':str(e)}),500

@app.route('/download', methods=['POST'])
def download_file():
    list_down = request.json.get('list_down')
    if not list_down:
        return jsonify({'error': 'List download files are unavailable'}), 400
    try:
        if peer.tit_for_tat <= 0:
            return jsonify({'message': 'you haven\'t shared enough files'}),404
        peer.download_files(list_down)
        peer.tit_for_tat-= len(list_down)
        return jsonify({'message': 'File names processed for download'}), 201
    except Exception:
        return jsonify({'error': 'Cannot download this list file'}), 404
@app.route('/stop', methods=['GET'])
def stop():
    try:
        peer.stop()
        return jsonify({'messsage':'This peer has disconnected to tracker'})
    except Exception:
        return jsonify({'error':'Cannot disconnected because some reason'})
if __name__ == '__main__':
    app.run(port=5000, debug=True)
