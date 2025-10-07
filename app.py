from flask import Flask, render_template
from admin_bp import admin_bp  # Import the admin blueprint
from attendance_app import attendance_bp
from target import target_bp
from user_technician import user_tech_bp
from tni_shared import tni_shared_bp
from ciro import ciro_bp
from feedback_form import feedback_bp
from cd_data_store import bp as cd_data_bp
from factory_data import factory_bp
from user_routes import user_bp
from user_auth import user_auth
from view_master_data import view_bp

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'

# Register all blueprints
app.register_blueprint(admin_bp, url_prefix='/admin')
app.register_blueprint(attendance_bp)
app.register_blueprint(target_bp)
app.register_blueprint(tni_shared_bp)
app.register_blueprint(factory_bp)
app.register_blueprint(user_bp)
app.register_blueprint(ciro_bp, url_prefix='/ciro')
app.register_blueprint(feedback_bp, url_prefix='/feedback')
app.register_blueprint(user_tech_bp, url_prefix='/user_tech')
app.register_blueprint(cd_data_bp)
app.register_blueprint(user_auth)
app.register_blueprint(view_bp)

@app.route('/')
def home():
    """Simple homepage with link to admin portal"""
    return render_template("homepage.html")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)