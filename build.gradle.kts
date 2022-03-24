plugins {
    kotlin("jvm") version "1.5.31"
    java
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))

    implementation("com.oracle.ojdbc:ojdbc8:19.3.0.0")

//    implementation("org.slf4j:slf4j-simple:2.0.0-alpha6")

    implementation("ch.qos.logback:logback-classic:1.3.0-alpha5")


    implementation("io.debezium:debezium-api:1.9.0.Alpha2")
    implementation("io.debezium:debezium-embedded:1.9.0.Alpha2")
    implementation("io.debezium:debezium-connector-oracle:1.9.0.Alpha2")


    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}