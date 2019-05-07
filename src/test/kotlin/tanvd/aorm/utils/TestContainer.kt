package tanvd.aorm.utils

import org.testcontainers.containers.GenericContainer

class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)
