from krc.env import EnvVar
 
def setup():
    email_password.value = raw_input('Email Password: ')

email_password = EnvVar('EmailPassword')

if __name__ == '__main__':
    setup()