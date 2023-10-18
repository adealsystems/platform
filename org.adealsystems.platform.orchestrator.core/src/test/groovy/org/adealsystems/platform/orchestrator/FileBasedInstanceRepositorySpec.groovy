package org.adealsystems.platform.orchestrator

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

class FileBasedInstanceRepositorySpec extends Specification {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedInstanceRepositorySpec)

    @TempDir
    File baseDirectory

    def 'instance works as expected'() {
        given:
        InstanceId instanceId = new InstanceId('0001-instance-id')
        def instanceConfiguration = [
            'aaa': 'xxx',
            'bbb': 'yyy'
        ]

        FileBasedInstanceRepository instance = new FileBasedInstanceRepository(baseDirectory)

        when:
        def allIds = instance.retrieveInstanceIds()

        then:
        allIds == [] as Set

        when:
        def i = instance.createInstance(instanceId)

        then:
        i != null
        i.id == instanceId
        i.configuration == null

        when:
        allIds = instance.retrieveInstanceIds()

        then:
        allIds == [instanceId] as Set

        when:
        def iWithConfig = new Instance(instanceId)
        iWithConfig.setConfiguration(instanceConfiguration)
        instance.updateInstance(iWithConfig)
        def otherI = instance.retrieveInstance(instanceId)

        then:
        otherI.present
        otherI.get().id == instanceId
        otherI.get().configuration == instanceConfiguration

        when:
        def lines = readLines(instance, instanceId)

        then:
        lines == [
            '{',
            '  "id" : "0001-instance-id",',
            '  "configuration" : {',
            '    "aaa" : "xxx",',
            '    "bbb" : "yyy"',
            '  }',
            '}',
        ]

        when:
        def existingI = instance.retrieveOrCreateInstance(instanceId)

        then:
        existingI.id == instanceId
        existingI.configuration == instanceConfiguration

        when:
        def deleted = instance.deleteInstance(instanceId)

        then:
        deleted

        when:
        def oneMoreI = instance.retrieveInstance(instanceId)

        then:
        !oneMoreI.present

        when:
        def freshI = instance.retrieveOrCreateInstance(instanceId)

        then:
        freshI.id == instanceId
        freshI.configuration == null
    }

    private static List<String> readLines(FileBasedInstanceRepository instance, InstanceId id) {
        File file = instance.getInstanceFile(id)
        LOGGER.info("Reading lines from '{}'", file.getAbsolutePath())
        return file.readLines()
    }
}
