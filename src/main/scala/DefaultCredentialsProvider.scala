package aws

import com.amazonaws.auth.AWSCredentials

class DefaultCredentialsProvider extends CredentialsProvider {
  private val provider = new com.amazonaws.auth.DefaultAWSCredentialsProviderChain
  override def getCredentials: Credentials = {
    provider.getCredentials match {
      case credentials: com.amazonaws.auth.AWSSessionCredentials ⇒
        Credentials(credentials.getAWSAccessKeyId, credentials.getAWSSecretKey, credentials.getSessionToken)

      case credentials: AWSCredentials ⇒
        Credentials(credentials.getAWSAccessKeyId, credentials.getAWSSecretKey)
    }
  }
  override def refresh: Unit = provider.refresh
}

object DefaultCredentialsProvider {
  def apply(): DefaultCredentialsProvider =
    new DefaultCredentialsProvider
}
