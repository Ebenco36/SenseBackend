from flask_restful import Resource, Api, reqparse
from flask_mail import Mail, Message

class ContactFormResource(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('name', type=str, required=True, help="Name is required")
        self.parser.add_argument('email', type=str, required=True, help="Email is required")
        self.parser.add_argument('message', type=str, required=True, help="Message is required")
        self.parser.add_argument('subject', type=str, required=False, help="Subject is optional")

    def post(self):
        """
        Send contact form details via email.
        """
        try:
            args = self.parser.parse_args()
            name = args['name']
            email = args['email']
            message = args['message']
            subject = args.get('subject', 'No Subject')

            # Create the email
            msg = Message(
                subject=f"Contact Form Submission: {subject}",
                sender=email,
                recipients=["recipient@example.com"]  # Replace with the recipient email address
            )
            msg.body = f"""
            You have received a new contact form submission:

            Name: {name}
            Email: {email}
            Subject: {subject}
            Message:
            {message}
            """

            # Send the email
            mail.send(msg)

            return {"success": True, "message": "Contact form submitted successfully."}, 200

        except Exception as e:
            return {"success": False, "error": str(e)}, 500