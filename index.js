const express = require("express");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");
const { MongoClient, ObjectId, ServerApiVersion } = require("mongodb");
const nodemailer = require("nodemailer");
const admin = require("firebase-admin");
const QRCode = require("qrcode");
require("dotenv").config();

const port = process.env.PORT || 3000;
const app = express();

app.use(cors());
app.use(express.json());

// ---------- Firebase Admin Initialization ----------
if (!admin.apps.length) {
  const serviceAccount = require("./complainly-firebase-adminsdk.json");
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}

// ---------- Nodemailer Transporter ----------
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS,
  },
  tls: {
    rejectUnauthorized: false
  }
});

transporter.verify(function (error, success) {
  if (error) {
    console.error("Nodemailer connection error:", error);
  } else {
    console.log("Nodemailer is ready!");
  }
});

const uri = process.env.MONGO_URI;

const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  }
});

async function run() {
  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db("complainly");
    const userCollection = db.collection("users");
    const formsCollection = db.collection("forms");
    const moderatorsCollection = db.collection("moderators");
    const complaintsCollection = db.collection("complaints");
    const departmentsCollection = db.collection("departments");
    const positionsCollection = db.collection("positions");
    const formAssignmentsCollection = db.collection("formAssignments");
    const moderatorAssignmentsCollection = db.collection("moderatorAssignments");
    const qrCodesCollection = db.collection("qrCodes");
    const messagesCollection = db.collection("messages");
    const notificationsCollection = db.collection("notifications");

    // ---------- Index Setup ----------
    try {
      await formsCollection.dropIndex("organizationUsername_1_name_1");
    } catch (e) { }
    await formsCollection.createIndex(
      { organizationUsername: 1, name: 1 },
      { unique: true },
    );

    await departmentsCollection.createIndex(
      { organizationUsername: 1, name: 1 },
      { unique: true }
    );

    await positionsCollection.createIndex(
      { departmentId: 1, name: 1 },
      { unique: true }
    );

    await messagesCollection.createIndex({ complaintId: 1, createdAt: 1 });

    // Notification indexes
    await notificationsCollection.createIndex({ organizationUsername: 1, createdAt: -1 });
    await notificationsCollection.createIndex({ recipientEmail: 1, read: 1 });

    console.log("Indexes ready");

    // ==================== SOCKET.IO ====================
    const server = http.createServer(app);
    const io = new Server(server, {
      cors: {
        origin: process.env.FRONTEND_URL || "https://client-complainly.vercel.app",
        methods: ["GET", "POST"]
      }
    });

    io.on("connection", (socket) => {
      console.log("User connected:", socket.id);

      // oin organization room for notifications
      socket.on("join_org", (organizationUsername) => {
        socket.join(`org_${organizationUsername}`);
      });

      // Join moderator room for notifications
      socket.on("join_mod", (moderatorEmail) => {
        socket.join(`mod_${moderatorEmail.replace(/[@.]/g, '_')}`);
      });

      socket.on("join_room", (complaintId) => {
        socket.join(complaintId);
        console.log(`User ${socket.id} joined room: ${complaintId}`);
      });

      socket.on("leave_room", (complaintId) => {
        socket.leave(complaintId);
      });

      socket.on("send_message", async (data) => {
        try {
          const { complaintId, senderEmail, senderRole, senderName, message } = data;
          const msgDoc = {
            complaintId,
            senderEmail,
            senderRole: senderRole || "user",
            senderName: senderName || senderEmail,
            message,
            read: false,
            createdAt: new Date(),
          };
          await messagesCollection.insertOne(msgDoc);
          io.to(complaintId).emit("receive_message", msgDoc);
        } catch (error) {
          console.error("Socket message error:", error);
        }
      });

      socket.on("typing", (data) => {
        const { complaintId, senderEmail, isTyping } = data;
        socket.to(complaintId).emit("user_typing", { senderEmail, isTyping });
      });

      socket.on("disconnect", () => {
        console.log("User disconnected:", socket.id);
      });
    });

    // ==================== MESSAGES ENDPOINTS ====================
    app.get("/messages/:complaintId", async (req, res) => {
      try {
        const { complaintId } = req.params;
        const messages = await messagesCollection
          .find({ complaintId })
          .sort({ createdAt: 1 })
          .toArray();
        res.send({ success: true, data: messages });
      } catch (error) {
        console.error("Error fetching messages:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.post("/messages", async (req, res) => {
      try {
        const { complaintId, senderEmail, senderRole, senderName, message } = req.body;
        if (!complaintId || !senderEmail || !message) {
          return res.status(400).send({ message: "Missing required fields" });
        }

        const msgDoc = {
          complaintId,
          senderEmail,
          senderRole: senderRole || "user",
          senderName: senderName || senderEmail,
          message,
          read: false,
          createdAt: new Date(),
        };

        const result = await messagesCollection.insertOne(msgDoc);
        res.status(201).send({ success: true, data: { _id: result.insertedId, ...msgDoc } });
      } catch (error) {
        console.error("Error sending message:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== NOTIFICATIONS ENDPOINTS ====================
    // Get notifications for org or moderator
    app.get("/notifications", async (req, res) => {
      try {
        const { organizationUsername, recipientEmail, limit = 20 } = req.query;
        let query = {};
        if (organizationUsername) query.organizationUsername = organizationUsername;
        if (recipientEmail) query.recipientEmail = recipientEmail;

        const notifications = await notificationsCollection
          .find(query)
          .sort({ createdAt: -1 })
          .limit(parseInt(limit))
          .toArray();

        const unreadCount = await notificationsCollection.countDocuments({
          ...query,
          read: false
        });

        res.send({ success: true, data: notifications, unreadCount });
      } catch (error) {
        console.error("Error fetching notifications:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // Mark single notification as read
    app.patch("/notifications/:id/read", async (req, res) => {
      try {
        const { id } = req.params;
        if (!ObjectId.isValid(id)) return res.status(400).send({ message: "Invalid ID" });

        await notificationsCollection.updateOne(
          { _id: new ObjectId(id) },
          { $set: { read: true, readAt: new Date() } }
        );
        res.send({ success: true, message: "Marked as read" });
      } catch (error) {
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // Mark all notifications as read
    app.patch("/notifications/read-all", async (req, res) => {
      try {
        const { organizationUsername, recipientEmail } = req.body;
        let query = {};
        if (organizationUsername) query.organizationUsername = organizationUsername;
        if (recipientEmail) query.recipientEmail = recipientEmail;

        await notificationsCollection.updateMany(query, { $set: { read: true, readAt: new Date() } });
        res.send({ success: true, message: "All marked as read" });
      } catch (error) {
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== ANALYTICS / DASHBOARD STATS ====================
    app.get("/analytics/:organizationUsername", async (req, res) => {
      try {
        const { organizationUsername } = req.params;
        const { days = 7 } = req.query;

        const startDate = new Date();
        startDate.setDate(startDate.getDate() - parseInt(days));

        // Total counts
        const totalComplaints = await complaintsCollection.countDocuments({ organizationUsername });
        const pendingCount = await complaintsCollection.countDocuments({ organizationUsername, status: "pending" });
        const workingCount = await complaintsCollection.countDocuments({ organizationUsername, status: "working" });
        const solvedCount = await complaintsCollection.countDocuments({ organizationUsername, status: "solved" });

        // Daily trend
        const dailyTrend = await complaintsCollection.aggregate([
          { $match: { organizationUsername, createdAt: { $gte: startDate } } },
          { $group: { _id: { $dateToString: { format: "%Y-%m-%d", date: "$createdAt" } }, count: { $sum: 1 } } },
          { $sort: { _id: 1 } }
        ]).toArray();

        // By department
        const byDepartment = await complaintsCollection.aggregate([
          { $match: { organizationUsername } },
          { $group: { _id: "$departmentId", count: { $sum: 1 } } }
        ]).toArray();

        const departmentStats = await Promise.all(
          byDepartment.map(async (item) => {
            let name = "General";
            if (item._id) {
              const dept = await departmentsCollection.findOne({ _id: new ObjectId(item._id) });
              name = dept?.name || "Unknown";
            }
            return { name, count: item.count };
          })
        );

        const byStatus = [
          { name: "Pending", value: pendingCount },
          { name: "Working", value: workingCount },
          { name: "Solved", value: solvedCount },
        ];

        // Average response time (hours)
        const avgResponse = await complaintsCollection.aggregate([
          { $match: { organizationUsername, status: { $in: ["working", "solved"] } } },
          { $unwind: "$history" },
          { $match: { "history.status": "working" } },
          { $group: { _id: null, avg: { $avg: { $subtract: ["$history.changedAt", "$createdAt"] } } } }
        ]).toArray();

        res.send({
          success: true,
          data: {
            totalComplaints,
            pendingCount,
            workingCount,
            solvedCount,
            dailyTrend,
            departmentStats,
            byStatus,
            avgResponseTime: avgResponse[0]?.avg ? Math.round(avgResponse[0].avg / 3600000) : null,
          }
        });
      } catch (error) {
        console.error("Error fetching analytics:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== QR CODE ENDPOINTS ====================
    app.post("/qr/generate", async (req, res) => {
      try {
        const { formId, organizationUsername } = req.body;

        if (!formId || !organizationUsername) {
          return res.status(400).send({ message: "Form ID and organization username are required" });
        }

        const form = await formsCollection.findOne({ _id: new ObjectId(formId) });
        if (!form) {
          return res.status(404).send({ message: "Form not found" });
        }

        const formUrl = `${process.env.FRONTEND_URL || 'https://client-complainly.vercel.app'}/form/${formId}`;

        const qrDataUrl = await QRCode.toDataURL(formUrl, {
          width: 400,
          margin: 2,
          color: {
            dark: '#000000',
            light: '#ffffff'
          }
        });

        const existingQR = await qrCodesCollection.findOne({ formId });

        if (existingQR) {
          await qrCodesCollection.updateOne(
            { formId },
            {
              $set: {
                qrDataUrl,
                formUrl,
                updatedAt: new Date()
              }
            }
          );
        } else {
          await qrCodesCollection.insertOne({
            formId,
            organizationUsername,
            formUrl,
            qrDataUrl,
            formName: form.name,
            createdAt: new Date(),
            updatedAt: new Date()
          });
        }

        res.send({
          success: true,
          data: {
            formId,
            formUrl,
            qrDataUrl,
            formName: form.name
          }
        });
      } catch (error) {
        console.error("Error generating QR code:", error);
        res.status(500).send({ message: "Failed to generate QR code" });
      }
    });

    app.get("/qr/:formId", async (req, res) => {
      try {
        const { formId } = req.params;
        const { organizationUsername } = req.query;

        let query = { formId };
        if (organizationUsername) query.organizationUsername = organizationUsername;

        const qrCode = await qrCodesCollection.findOne(query);

        if (!qrCode) {
          return res.status(404).send({ message: "QR code not found" });
        }

        res.send({ success: true, data: qrCode });
      } catch (error) {
        console.error("Error fetching QR code:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/qr/organization/:organizationUsername", async (req, res) => {
      try {
        const { organizationUsername } = req.params;

        const qrCodes = await qrCodesCollection
          .find({ organizationUsername })
          .toArray();

        res.send({ success: true, data: qrCodes });
      } catch (error) {
        console.error("Error fetching QR codes:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.delete("/qr/:formId", async (req, res) => {
      try {
        const { formId } = req.params;
        const { organizationUsername } = req.query;

        const result = await qrCodesCollection.deleteOne({
          formId,
          organizationUsername
        });

        if (result.deletedCount === 0) {
          return res.status(404).send({ message: "QR code not found" });
        }

        res.send({ success: true, message: "QR code deleted" });
      } catch (error) {
        console.error("Error deleting QR code:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.put("/qr/:formId", async (req, res) => {
      try {
        const { formId } = req.params;
        const { organizationUsername, logoUrl, primaryColor, backgroundColor } = req.body;

        let qrCode = await qrCodesCollection.findOne({ formId, organizationUsername });

        if (!qrCode) {
          return res.status(404).send({ message: "QR code not found. Generate it first." });
        }

        const formUrl = qrCode.formUrl;
        const qrOptions = {
          width: 400,
          margin: 2,
          color: {
            dark: primaryColor || '#000000',
            light: backgroundColor || '#ffffff'
          }
        };

        const qrDataUrl = await QRCode.toDataURL(formUrl, qrOptions);

        await qrCodesCollection.updateOne(
          { formId, organizationUsername },
          {
            $set: {
              qrDataUrl,
              logoUrl: logoUrl || null,
              primaryColor: primaryColor || '#000000',
              backgroundColor: backgroundColor || '#ffffff',
              updatedAt: new Date()
            }
          }
        );

        res.send({
          success: true,
          data: {
            formId,
            formUrl,
            qrDataUrl,
            logoUrl,
            primaryColor,
            backgroundColor
          }
        });
      } catch (error) {
        console.error("Error updating QR code:", error);
        res.status(500).send({ message: "Failed to update QR code" });
      }
    });

    // ==================== USER ENDPOINTS ====================
    app.post("/users", async (req, res) => {
      try {
        const {
          name,
          email,
          organizationName,
          organizationUsername,
          organizationLogo,
          userRole,
        } = req.body;

        if (!email || !name) {
          return res.status(400).send({ message: "Missing required fields" });
        }
        if (userRole !== "user" && userRole !== "organization") {
          return res.status(400).send({ message: "Invalid user type" });
        }
        if (
          userRole === "organization" &&
          (!organizationName || !organizationUsername)
        ) {
          return res.status(400).send({ message: "Missing organization data" });
        }

        const existingEmail = await userCollection.findOne({ email });
        if (existingEmail) {
          return res.status(409).send({ message: "Email already registered" });
        }

        if (userRole === "organization" && organizationUsername) {
          const existingOrg = await userCollection.findOne({
            userRole: "organization",
            organizationUsername,
          });
          if (existingOrg) {
            return res.status(409).send({ message: "Organization username already taken" });
          }
        }

        const userDocument = {
          name,
          email,
          userRole,
          organizationName: organizationName || null,
          organizationUsername: organizationUsername || null,
          organizationLogo: organizationLogo || null,
          createdAt: new Date(),
        };

        const result = await userCollection.insertOne(userDocument);
        res.status(201).send(result);
      } catch (error) {
        console.error("Error in POST /users:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.post("/users/check-existence", async (req, res) => {
      try {
        const { email, organizationUsername } = req.body;
        const checks = {};

        if (email) {
          const existingEmail = await userCollection.findOne({ email });
          checks.emailExists = !!existingEmail;
        }

        if (organizationUsername) {
          const existingOrg = await userCollection.findOne({
            userRole: "organization",
            organizationUsername
          });
          checks.usernameExists = !!existingOrg;
        }

        checks.exists = checks.emailExists || checks.usernameExists;
        res.status(200).send(checks);
      } catch (error) {
        console.error("Error in POST /users/check-existence:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/users/:email", async (req, res) => {
      const email = req.params.email;
      const result = await userCollection.findOne({ email: email });
      res.send(result);
    });

    app.put("/users/profile/:email", async (req, res) => {
      try {
        const { email } = req.params;
        const { name, organizationName, organizationLogo } = req.body;

        const user = await userCollection.findOne({ email });
        if (!user) {
          return res.status(404).send({ message: "User not found" });
        }

        const updateData = {
          name,
          updatedAt: new Date(),
        };

        if (user.userRole === "organization") {
          if (organizationName) updateData.organizationName = organizationName;
          if (organizationLogo) updateData.organizationLogo = organizationLogo;
        }

        const result = await userCollection.updateOne(
          { email },
          { $set: updateData }
        );

        res.send({
          success: true,
          message: "Profile updated successfully",
          data: updateData,
        });
      } catch (error) {
        console.error("Error updating profile:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== DEPARTMENT ENDPOINTS ====================
    app.post("/departments", async (req, res) => {
      try {
        const { name, description, organizationUsername } = req.body;

        if (!name || !organizationUsername) {
          return res.status(400).send({ message: "Missing required fields" });
        }

        const existingDept = await departmentsCollection.findOne({
          organizationUsername,
          name: { $regex: new RegExp(`^${name}$`, "i") }
        });

        if (existingDept) {
          return res.status(409).send({ message: "Department already exists" });
        }

        const department = {
          name,
          description: description || "",
          organizationUsername,
          createdAt: new Date(),
          updatedAt: new Date()
        };

        const result = await departmentsCollection.insertOne(department);
        res.status(201).send({ success: true, data: { _id: result.insertedId, ...department } });
      } catch (error) {
        console.error("Error creating department:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/departments", async (req, res) => {
      try {
        const { organizationUsername } = req.query;
        if (!organizationUsername) {
          return res.status(400).send({ message: "organizationUsername is required" });
        }

        const departments = await departmentsCollection
          .find({ organizationUsername })
          .sort({ createdAt: -1 })
          .toArray();

        const departmentsWithPositions = await Promise.all(
          departments.map(async (dept) => {
            const positions = await positionsCollection
              .find({ departmentId: dept._id.toString() })
              .toArray();
            return { ...dept, positions };
          })
        );

        res.send({ success: true, data: departmentsWithPositions });
      } catch (error) {
        console.error("Error fetching departments:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.put("/departments/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { name, description, organizationUsername } = req.body;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid department ID" });
        }

        const result = await departmentsCollection.updateOne(
          { _id: new ObjectId(id), organizationUsername },
          { $set: { name, description, updatedAt: new Date() } }
        );

        if (result.matchedCount === 0) {
          return res.status(404).send({ message: "Department not found" });
        }

        res.send({ success: true, message: "Department updated successfully" });
      } catch (error) {
        console.error("Error updating department:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.delete("/departments/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { organizationUsername } = req.query;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid department ID" });
        }

        const positions = await positionsCollection.find({ departmentId: id }).toArray();
        if (positions.length > 0) {
          return res.status(400).send({
            message: "Cannot delete department with existing positions. Delete positions first."
          });
        }

        const result = await departmentsCollection.deleteOne({
          _id: new ObjectId(id),
          organizationUsername
        });

        if (result.deletedCount === 0) {
          return res.status(404).send({ message: "Department not found" });
        }

        res.send({ success: true, message: "Department deleted successfully" });
      } catch (error) {
        console.error("Error deleting department:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== POSITION ENDPOINTS ====================
    app.post("/positions", async (req, res) => {
      try {
        const { name, description, departmentId, organizationUsername } = req.body;

        if (!name || !departmentId || !organizationUsername) {
          return res.status(400).send({ message: "Missing required fields" });
        }

        const existingPosition = await positionsCollection.findOne({
          departmentId,
          name: { $regex: new RegExp(`^${name}$`, "i") }
        });

        if (existingPosition) {
          return res.status(409).send({ message: "Position already exists in this department" });
        }

        const position = {
          name,
          description: description || "",
          departmentId,
          organizationUsername,
          createdAt: new Date(),
          updatedAt: new Date()
        };

        const result = await positionsCollection.insertOne(position);
        res.status(201).send({ success: true, data: { _id: result.insertedId, ...position } });
      } catch (error) {
        console.error("Error creating position:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/positions", async (req, res) => {
      try {
        const { departmentId, organizationUsername } = req.query;

        let query = {};
        if (departmentId) query.departmentId = departmentId;
        if (organizationUsername) query.organizationUsername = organizationUsername;

        const positions = await positionsCollection
          .find(query)
          .sort({ createdAt: -1 })
          .toArray();

        res.send({ success: true, data: positions });
      } catch (error) {
        console.error("Error fetching positions:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.put("/positions/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { name, description, organizationUsername } = req.body;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid position ID" });
        }

        const result = await positionsCollection.updateOne(
          { _id: new ObjectId(id), organizationUsername },
          { $set: { name, description, updatedAt: new Date() } }
        );

        if (result.matchedCount === 0) {
          return res.status(404).send({ message: "Position not found" });
        }

        res.send({ success: true, message: "Position updated successfully" });
      } catch (error) {
        console.error("Error updating position:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.delete("/positions/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { organizationUsername } = req.query;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid position ID" });
        }

        const result = await positionsCollection.deleteOne({
          _id: new ObjectId(id),
          organizationUsername
        });

        if (result.deletedCount === 0) {
          return res.status(404).send({ message: "Position not found" });
        }

        res.send({ success: true, message: "Position deleted successfully" });
      } catch (error) {
        console.error("Error deleting position:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== FORM ASSIGNMENT ENDPOINTS ====================
    app.post("/form-assignments", async (req, res) => {
      try {
        const { formId, departmentId, positionId, organizationUsername } = req.body;

        if (!formId || !organizationUsername) {
          return res.status(400).send({ message: "Missing required fields" });
        }

        const existingAssignment = await formAssignmentsCollection.findOne({
          formId,
          departmentId: departmentId || null,
          positionId: positionId || null,
          organizationUsername
        });

        if (existingAssignment) {
          return res.status(409).send({
            success: false,
            message: "This form is already assigned to this department and position."
          });
        }

        const assignment = {
          formId,
          departmentId: departmentId || null,
          positionId: positionId || null,
          organizationUsername,
          createdAt: new Date(),
          updatedAt: new Date()
        };

        const result = await formAssignmentsCollection.insertOne(assignment);
        res.status(201).send({ success: true, data: { _id: result.insertedId, ...assignment } });
      } catch (error) {
        console.error("Error creating form assignment:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/form-assignments", async (req, res) => {
      try {
        const { organizationUsername, departmentId, positionId } = req.query;

        let query = { organizationUsername };
        if (departmentId) query.departmentId = departmentId;
        if (positionId) query.positionId = positionId;

        const assignments = await formAssignmentsCollection
          .find(query)
          .toArray();

        const assignmentsWithForms = await Promise.all(
          assignments.map(async (assignment) => {
            const form = await formsCollection.findOne({ _id: new ObjectId(assignment.formId) });
            return { ...assignment, form };
          })
        );

        res.send({ success: true, data: assignmentsWithForms });
      } catch (error) {
        console.error("Error fetching form assignments:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.delete("/form-assignments/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { organizationUsername } = req.query;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid assignment ID" });
        }

        const result = await formAssignmentsCollection.deleteOne({
          _id: new ObjectId(id),
          organizationUsername
        });

        if (result.deletedCount === 0) {
          return res.status(404).send({ message: "Assignment not found" });
        }

        res.send({ success: true, message: "Assignment deleted successfully" });
      } catch (error) {
        console.error("Error deleting assignment:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== MODERATOR ASSIGNMENT ENDPOINTS ====================
    app.post("/moderator-assignments", async (req, res) => {
      try {
        const { moderatorId, departmentId, positionId, organizationUsername } = req.body;

        if (!moderatorId || !organizationUsername) {
          return res.status(400).send({ message: "Missing required fields" });
        }

        const existingAssignment = await moderatorAssignmentsCollection.findOne({
          moderatorId,
          departmentId: departmentId || null,
          positionId: positionId || null,
          organizationUsername
        });

        if (existingAssignment) {
          return res.status(409).send({
            success: false,
            message: "This moderator is already assigned to this department and position."
          });
        }

        const assignment = {
          moderatorId,
          departmentId: departmentId || null,
          positionId: positionId || null,
          organizationUsername,
          createdAt: new Date(),
          updatedAt: new Date()
        };

        const result = await moderatorAssignmentsCollection.insertOne(assignment);
        res.status(201).send({ success: true, data: { _id: result.insertedId, ...assignment } });
      } catch (error) {
        console.error("Error creating moderator assignment:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/moderator-assignments", async (req, res) => {
      try {
        const { organizationUsername, moderatorId, departmentId, positionId } = req.query;

        let query = { organizationUsername };
        if (moderatorId) query.moderatorId = moderatorId;
        if (departmentId) query.departmentId = departmentId;
        if (positionId) query.positionId = positionId;

        const assignments = await moderatorAssignmentsCollection
          .find(query)
          .toArray();

        const assignmentsWithDetails = await Promise.all(
          assignments.map(async (assignment) => {
            const moderator = await userCollection.findOne({
              email: assignment.moderatorId,
              userRole: "moderator"
            });
            let department = null;
            let position = null;

            if (assignment.departmentId) {
              department = await departmentsCollection.findOne({
                _id: new ObjectId(assignment.departmentId)
              });
            }
            if (assignment.positionId) {
              position = await positionsCollection.findOne({
                _id: new ObjectId(assignment.positionId)
              });
            }

            return { ...assignment, moderator, department, position };
          })
        );

        res.send({ success: true, data: assignmentsWithDetails });
      } catch (error) {
        console.error("Error fetching moderator assignments:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.put("/moderator-assignments/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { moderatorId, departmentId, positionId, organizationUsername } = req.body;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid assignment ID" });
        }

        const duplicate = await moderatorAssignmentsCollection.findOne({
          _id: { $ne: new ObjectId(id) },
          moderatorId,
          departmentId: departmentId || null,
          positionId: positionId || null,
          organizationUsername
        });

        if (duplicate) {
          return res.status(409).send({
            success: false,
            message: "This moderator is already assigned to this department and position."
          });
        }

        const result = await moderatorAssignmentsCollection.updateOne(
          { _id: new ObjectId(id), organizationUsername },
          {
            $set: {
              departmentId: departmentId || null,
              positionId: positionId || null,
              updatedAt: new Date()
            }
          }
        );

        if (result.matchedCount === 0) {
          return res.status(404).send({ message: "Assignment not found" });
        }

        res.send({ success: true, message: "Assignment updated successfully" });
      } catch (error) {
        console.error("Error updating assignment:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.delete("/moderator-assignments/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { organizationUsername } = req.query;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid assignment ID" });
        }

        const result = await moderatorAssignmentsCollection.deleteOne({
          _id: new ObjectId(id),
          organizationUsername
        });

        if (result.deletedCount === 0) {
          return res.status(404).send({ message: "Assignment not found" });
        }

        res.send({ success: true, message: "Assignment deleted successfully" });
      } catch (error) {
        console.error("Error deleting assignment:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== MODERATOR ENDPOINTS ====================
    app.post("/moderators", async (req, res) => {
      try {
        const { name, email, organizationUsername } = req.body;
        if (!name || !email || !organizationUsername) {
          return res.status(400).send({ message: "Missing required fields" });
        }

        const existingUser = await userCollection.findOne({ email });
        if (existingUser) {
          return res.status(409).send({ message: "Email already registered" });
        }

        const existingMod = await moderatorsCollection.findOne({
          email,
          organizationUsername,
        });
        if (existingMod) {
          return res.status(409).send({
            message: "Moderator already exists for this organization",
          });
        }

        const generatedPassword = Math.random().toString(36).slice(-10) + "A1!@#";

        let firebaseUser;
        try {
          firebaseUser = await admin.auth().createUser({
            email,
            password: generatedPassword,
            displayName: name,
          });
        } catch (fbError) {
          console.error("Firebase create user error:", fbError);
          return res.status(500).send({ message: "Failed to create Firebase user" });
        }

        const moderatorData = {
          name,
          email,
          organizationUsername,
          role: "moderator",
          firebaseUid: firebaseUser.uid,
          createdAt: new Date(),
        };
        const result = await moderatorsCollection.insertOne(moderatorData);

        await userCollection.insertOne({
          name,
          email,
          userRole: "moderator",
          organizationUsername,
          firebaseUid: firebaseUser.uid,
          createdAt: new Date(),
        });

        console.log("Sending mail to:", email);
        console.log("Sender Email User:", process.env.EMAIL_USER);
        console.log("Sender Email Pass exists:", !!process.env.EMAIL_PASS);

        const mailOptions = {
          from: `"Complainly" <${process.env.EMAIL_USER}>`,
          to: email,
          subject: "Your Moderator Account for Complainly",
          html: `
            <h2>Welcome, ${name}!</h2>
            <p>You have been added as a moderator for <strong>${organizationUsername}</strong>.</p>
            <p>Your login credentials:</p>
            <ul>
              <li><strong>Email:</strong> ${email}</li>
              <li><strong>Password:</strong> ${generatedPassword}</li>
            </ul>
            <p>Please log in and change your password after first login.</p>
          `,
        };

        await transporter.sendMail(mailOptions);

        res.status(201).send({
          success: true,
          message: "Moderator created and email sent",
          data: { id: result.insertedId, email, name },
        });
      } catch (error) {
        console.error("Error creating moderator:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/moderators", async (req, res) => {
      try {
        const { organizationUsername } = req.query;
        if (!organizationUsername) {
          return res.status(400).send({ message: "organizationUsername is required" });
        }
        const moderators = await moderatorsCollection
          .find({ organizationUsername })
          .project({ firebaseUid: 0 })
          .toArray();
        res.send({ success: true, data: moderators });
      } catch (error) {
        console.error("Error fetching moderators:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.delete("/moderators/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { organizationUsername } = req.query;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid ID" });
        }

        const moderator = await moderatorsCollection.findOne({
          _id: new ObjectId(id),
        });
        if (!moderator) {
          return res.status(404).send({ message: "Moderator not found" });
        }

        if (moderator.organizationUsername !== organizationUsername) {
          return res.status(403).send({ message: "Unauthorized to delete this moderator" });
        }

        if (moderator.firebaseUid) {
          try {
            await admin.auth().deleteUser(moderator.firebaseUid);
          } catch (fbErr) {
            console.warn("Firebase user deletion failed:", fbErr.message);
          }
        }

        await moderatorsCollection.deleteOne({ _id: new ObjectId(id) });
        await userCollection.deleteOne({ email: moderator.email });
        await moderatorAssignmentsCollection.deleteMany({ moderatorId: moderator.email });

        res.send({ success: true, message: "Moderator deleted successfully" });
      } catch (error) {
        console.error("Error deleting moderator:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== FORM ENDPOINTS ====================
    app.post("/forms", async (req, res) => {
      try {
        const { name, fields, organizationUsername, createdAt } = req.body;
        if (!name || !fields || !organizationUsername) {
          return res.status(400).send({ message: "Missing required fields" });
        }

        const existingForm = await formsCollection.findOne({
          organizationUsername,
          name: { $regex: new RegExp(`^${name}$`, "i") },
        });

        if (existingForm) {
          return res.status(409).send({
            success: false,
            message: "A form with this name already exists.",
          });
        }

        const formData = {
          name,
          fields,
          organizationUsername,
          createdAt: createdAt || new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        };

        const result = await formsCollection.insertOne(formData);
        res.status(201).send({
          success: true,
          data: { id: result.insertedId, ...formData },
        });
      } catch (error) {
        console.error("Error in POST /forms:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/forms", async (req, res) => {
      try {
        const { organizationUsername } = req.query;
        if (!organizationUsername) {
          return res.status(400).send({ message: "organizationUsername is required" });
        }
        const forms = await formsCollection
          .find({ organizationUsername })
          .toArray();
        res.send({ success: true, data: forms });
      } catch (error) {
        console.error("Error fetching forms:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/forms/:id", async (req, res) => {
      try {
        const { id } = req.params;
        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid form ID" });
        }
        const form = await formsCollection.findOne({ _id: new ObjectId(id) });
        if (!form) {
          return res.status(404).send({ message: "Form not found" });
        }
        res.send({ success: true, data: form });
      } catch (error) {
        console.error("Error fetching form:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.put("/forms/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { name, fields, organizationUsername } = req.body;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid form ID" });
        }

        const existingForm = await formsCollection.findOne({
          organizationUsername,
          name: { $regex: new RegExp(`^${name}$`, "i") },
          _id: { $ne: new ObjectId(id) },
        });

        if (existingForm) {
          return res.status(409).send({
            success: false,
            message: "Another form with this name already exists.",
          });
        }

        const updateData = {
          name,
          fields,
          organizationUsername,
          updatedAt: new Date().toISOString(),
        };

        const result = await formsCollection.updateOne(
          { _id: new ObjectId(id) },
          { $set: updateData }
        );

        if (result.matchedCount === 0) {
          return res.status(404).send({ message: "Form not found" });
        }

        res.send({ success: true, data: { id, ...updateData } });
      } catch (error) {
        console.error("Error updating form:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.delete("/forms/:id", async (req, res) => {
      try {
        const { id } = req.params;
        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid form ID" });
        }

        await formAssignmentsCollection.deleteMany({ formId: id });
        await qrCodesCollection.deleteMany({ formId: id });
        const result = await formsCollection.deleteOne({ _id: new ObjectId(id) });

        if (result.deletedCount === 0) {
          return res.status(404).send({ message: "Form not found" });
        }

        res.send({ success: true, message: "Form deleted successfully" });
      } catch (error) {
        console.error("Error deleting form:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== COMPLAINT ENDPOINTS ====================
    // app.post("/public/complaints", async (req, res) => {
    //   try {
    //     const { formId, organizationUsername, formData, userEmail, departmentId, positionId } = req.body;

    //     if (!formId || !organizationUsername || !formData) {
    //       return res.status(400).send({ message: "Missing required fields" });
    //     }

    //     let user = await userCollection.findOne({ email: userEmail });
    //     if (!user && userEmail) {
    //       const newUser = {
    //         email: userEmail,
    //         userRole: "user",
    //         createdAt: new Date(),
    //       };
    //       const result = await userCollection.insertOne(newUser);
    //       user = { _id: result.insertedId, ...newUser };
    //     }

    //     // ---------- SMART ASSIGNMENT LOGIC ----------
    //     let assignedModerator = null;

    //     // If department selected, try to assign within that department/position
    //     if (departmentId) {
    //       let modQuery = { organizationUsername, departmentId };
    //       if (positionId) modQuery.positionId = positionId;

    //       const moderatorAssigns = await moderatorAssignmentsCollection.find(modQuery).toArray();
    //       if (moderatorAssigns.length > 0) {
    //         // get workloads
    //         const workloads = await Promise.all(
    //           moderatorAssigns.map(async (assign) => {
    //             const moderator = await userCollection.findOne({
    //               email: assign.moderatorId,
    //               userRole: "moderator"
    //             });
    //             if (!moderator) return null;
    //             const count = await complaintsCollection.countDocuments({
    //               assignedTo: moderator._id.toString(),
    //               status: { $in: ["pending", "working"] }
    //             });
    //             return { moderator, count };
    //           })
    //         );
    //         const valid = workloads.filter(w => w !== null);
    //         valid.sort((a, b) => a.count - b.count);
    //         assignedModerator = valid[0]?.moderator || null;
    //       }
    //     }

    //     // If still no moderator, check general moderators (no department assignment)
    //     if (!assignedModerator) {
    //       const allModerators = await userCollection
    //         .find({ userRole: "moderator", organizationUsername })
    //         .toArray();

    //       if (allModerators.length > 0) {
    //         // Find moderators who are NOT assigned to any specific department
    //         const generalModerators = [];
    //         for (const mod of allModerators) {
    //           const hasSpecificAssignment = await moderatorAssignmentsCollection.findOne({
    //             moderatorId: mod.email,
    //             departmentId: { $ne: null }
    //           });
    //           if (!hasSpecificAssignment) {
    //             generalModerators.push(mod);
    //           }
    //         }

    //         if (generalModerators.length > 0) {
    //           const workloads = await Promise.all(
    //             generalModerators.map(async (mod) => {
    //               const count = await complaintsCollection.countDocuments({
    //                 assignedTo: mod._id.toString(),
    //                 status: { $in: ["pending", "working"] }
    //               });
    //               return { moderator: mod, count };
    //             })
    //           );
    //           workloads.sort((a, b) => a.count - b.count);
    //           assignedModerator = workloads[0].moderator;
    //         } else {
    //           // No general moderators, pick least busy among all
    //           const workloads = await Promise.all(
    //             allModerators.map(async (mod) => {
    //               const count = await complaintsCollection.countDocuments({
    //                 assignedTo: mod._id.toString(),
    //                 status: { $in: ["pending", "working"] }
    //               });
    //               return { moderator: mod, count };
    //             })
    //           );
    //           workloads.sort((a, b) => a.count - b.count);
    //           assignedModerator = workloads[0].moderator;
    //         }
    //       }
    //     }

    //     // Fallback to organization owner
    //     if (!assignedModerator) {
    //       assignedModerator = await userCollection.findOne({
    //         userRole: "organization",
    //         organizationUsername
    //       });
    //     }

    //     const complaint = {
    //       formId: new ObjectId(formId),
    //       organizationUsername,
    //       userId: user?._id || null,
    //       userEmail: userEmail || null,
    //       formData,
    //       departmentId: departmentId || null,
    //       positionId: positionId || null,
    //       status: "pending",
    //       assignedTo: assignedModerator?._id.toString() || null,
    //       assignedToEmail: assignedModerator?.email || null,
    //       assignedToRole: assignedModerator?.userRole || null,
    //       createdAt: new Date(),
    //       updatedAt: new Date(),
    //       history: [
    //         {
    //           status: "pending",
    //           assignedTo: assignedModerator?._id.toString() || null,
    //           changedAt: new Date(),
    //           note: `Complaint submitted, assigned to ${assignedModerator?.name || "unassigned"}`,
    //         },
    //       ],
    //     };

    //     const result = await complaintsCollection.insertOne(complaint);

    //     // Create notification
    //     await notificationsCollection.insertOne({
    //       organizationUsername,
    //       recipientEmail: assignedModerator?.email || null,
    //       recipientRole: assignedModerator?.userRole || null,
    //       type: "new_complaint",
    //       title: "New Complaint Received",
    //       message: `New complaint from ${formData?.name || userEmail || "Anonymous"}`,
    //       complaintId: result.insertedId.toString(),
    //       read: false,
    //       createdAt: new Date(),
    //     });

    //     // Emit real-time notification to org room
    //     io.to(`org_${organizationUsername}`).emit("new_notification", {
    //       complaintId: result.insertedId,
    //       formName: formData?.formName || "Unknown",
    //       customerName: formData?.name || "Anonymous",
    //     });

    //     // Emit to assigned moderator if any
    //     if (assignedModerator?.email) {
    //       const modRoom = `mod_${assignedModerator.email.replace(/[@.]/g, '_')}`;
    //       io.to(modRoom).emit("new_notification", {
    //         complaintId: result.insertedId,
    //         customerName: formData?.name || "Anonymous",
    //       });
    //     }

    //     res.status(201).send({
    //       success: true,
    //       message: "Complaint submitted successfully",
    //       complaintId: result.insertedId,
    //     });
    //   } catch (error) {
    //     console.error("Error submitting complaint:", error);
    //     res.status(500).send({ message: "Internal server error" });
    //   }
    // });
    // ==================== COMPLAINT ROUTING (FIXED) ====================
    app.post("/public/complaints", async (req, res) => {
      try {
        const { formId, organizationUsername, formData, userEmail, departmentId, positionId } = req.body;

        if (!formId || !organizationUsername || !formData) {
          return res.status(400).send({ message: "Missing required fields" });
        }

        // Find or create lightweight user record
        let user = await userCollection.findOne({ email: userEmail });
        if (!user && userEmail) {
          const result = await userCollection.insertOne({
            email: userEmail,
            userRole: "user",
            createdAt: new Date(),
          });
          user = { _id: result.insertedId };
        }

        // etermine target department & position
        let targetDept = departmentId || null;
        let targetPos = positionId || null;

        // If the user didn’t select a department, check the form assignment
        if (!targetDept) {
          const formAssignment = await formAssignmentsCollection.findOne({ formId });
          if (formAssignment) {
            targetDept = formAssignment.departmentId || null;
            targetPos = formAssignment.positionId || null;
          }
        }

        let assignedModerator = null;

        // --------------------------------------------------
        // department/position is known
        // --------------------------------------------------
        if (targetDept) {
          // Try to find moderators specifically assigned to this dept (and pos if given)
          const modQuery = {
            organizationUsername,
            departmentId: targetDept,
          };
          if (targetPos) modQuery.positionId = targetPos;

          let eligibleAssignments = await moderatorAssignmentsCollection.find(modQuery).toArray();

          // If no moderator for the exact position, widen to whole department
          if (eligibleAssignments.length === 0 && targetPos) {
            eligibleAssignments = await moderatorAssignmentsCollection
              .find({ organizationUsername, departmentId: targetDept })
              .toArray();
          }

          if (eligibleAssignments.length > 0) {
            // Pick the least busy moderator among the eligible ones
            const workloads = await Promise.all(
              eligibleAssignments.map(async (assign) => {
                const mod = await userCollection.findOne({
                  email: assign.moderatorId,
                  userRole: "moderator",
                });
                if (!mod) return null;
                const count = await complaintsCollection.countDocuments({
                  assignedTo: mod._id.toString(),
                  status: { $in: ["pending", "working"] },
                });
                return { moderator: mod, count };
              })
            );
            const valid = workloads.filter((w) => w !== null);
            valid.sort((a, b) => a.count - b.count);
            assignedModerator = valid[0]?.moderator || null;
          }
        }

        // --------------------------------------------------
        // no department → look for general moderators
        // --------------------------------------------------
        if (!assignedModerator && !targetDept) {
          const allMods = await userCollection
            .find({ userRole: "moderator", organizationUsername })
            .toArray();

          if (allMods.length > 0) {
            // Separate moderators who have NO department assignment (general mods)
            const generalMods = [];
            for (const mod of allMods) {
              const assigned = await moderatorAssignmentsCollection.findOne({
                moderatorId: mod.email,
                departmentId: { $ne: null },
              });
              if (!assigned) generalMods.push(mod);
            }

            // If there are general moderators, pick least busy among them
            if (generalMods.length > 0) {
              const workloads = await Promise.all(
                generalMods.map(async (mod) => {
                  const count = await complaintsCollection.countDocuments({
                    assignedTo: mod._id.toString(),
                    status: { $in: ["pending", "working"] },
                  });
                  return { moderator: mod, count };
                })
              );
              workloads.sort((a, b) => a.count - b.count);
              assignedModerator = workloads[0].moderator;
            } else {
              // No general moderators – fallback to any moderator (already filtered, but just in case)
              const workloads = await Promise.all(
                allMods.map(async (mod) => {
                  const count = await complaintsCollection.countDocuments({
                    assignedTo: mod._id.toString(),
                    status: { $in: ["pending", "working"] },
                  });
                  return { moderator: mod, count };
                })
              );
              workloads.sort((a, b) => a.count - b.count);
              assignedModerator = workloads[0].moderator;
            }
          }
        }

        // --------------------------------------------------
        // no moderator at all → organisation owner
        // --------------------------------------------------
        if (!assignedModerator) {
          assignedModerator = await userCollection.findOne({
            userRole: "organization",
            organizationUsername,
          });
        }

        // --------------------------------------------------
        // Build & insert complaint
        // --------------------------------------------------
        const complaint = {
          formId: new ObjectId(formId),
          organizationUsername,
          userId: user?._id || null,
          userEmail: userEmail || null,
          formData,
          departmentId: targetDept,
          positionId: targetPos,
          status: "pending",
          assignedTo: assignedModerator?._id.toString() || null,
          assignedToEmail: assignedModerator?.email || null,
          assignedToRole: assignedModerator?.userRole || null,
          createdAt: new Date(),
          updatedAt: new Date(),
          history: [
            {
              status: "pending",
              assignedTo: assignedModerator?._id.toString() || null,
              changedAt: new Date(),
              note: `Complaint submitted, assigned to ${assignedModerator?.name || "unassigned"
                }`,
            },
          ],
        };

        const result = await complaintsCollection.insertOne(complaint);

        // Create notification & emit socket event
        await notificationsCollection.insertOne({
          organizationUsername,
          recipientEmail: assignedModerator?.email || null,
          recipientRole: assignedModerator?.userRole || null,
          type: "new_complaint",
          title: "New Complaint Received",
          message: `New complaint from ${formData?.name || userEmail || "Anonymous"}`,
          complaintId: result.insertedId.toString(),
          read: false,
          createdAt: new Date(),
        });

        io.to(`org_${organizationUsername}`).emit("new_notification", {
          complaintId: result.insertedId,
          customerName: formData?.name || "Anonymous",
        });

        if (assignedModerator?.email) {
          const modRoom = `mod_${assignedModerator.email.replace(/[@.]/g, "_")}`;
          io.to(modRoom).emit("new_notification", {
            complaintId: result.insertedId,
            customerName: formData?.name || "Anonymous",
          });
        }

        res.status(201).send({
          success: true,
          message: "Complaint submitted successfully",
          complaintId: result.insertedId,
        });
      } catch (error) {
        console.error("Error submitting complaint:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/public/complaints/:id", async (req, res) => {
      try {
        const { id } = req.params;
        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid complaint ID" });
        }

        const complaint = await complaintsCollection.findOne({
          _id: new ObjectId(id),
        });

        if (!complaint) {
          return res.status(404).send({ message: "Complaint not found" });
        }

        const form = await formsCollection.findOne({
          _id: complaint.formId,
        });

        res.send({
          success: true,
          data: {
            ...complaint,
            formName: form?.name || "Unknown Form",
            status: complaint.status,
            createdAt: complaint.createdAt,
            history: complaint.history || [],
          },
        });
      } catch (error) {
        console.error("Error fetching complaint:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/public/complaints/user/:email", async (req, res) => {
      try {
        const { email } = req.params;
        const complaints = await complaintsCollection
          .find({ userEmail: email })
          .sort({ createdAt: -1 })
          .toArray();

        const complaintsWithFormNames = await Promise.all(
          complaints.map(async (complaint) => {
            const form = await formsCollection.findOne({
              _id: complaint.formId,
            });
            return {
              ...complaint,
              formName: form?.name || "Unknown Form",
            };
          })
        );

        res.send({ success: true, data: complaintsWithFormNames });
      } catch (error) {
        console.error("Error fetching user complaints:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/complaints", async (req, res) => {
      try {
        const { organizationUsername, moderatorEmail, status, departmentId, positionId } = req.query;

        let query = {};
        if (organizationUsername) query.organizationUsername = organizationUsername;
        if (moderatorEmail) query.assignedToEmail = moderatorEmail;
        if (status && status !== "all") query.status = status;
        if (departmentId) query.departmentId = departmentId;
        if (positionId) query.positionId = positionId;

        const complaints = await complaintsCollection
          .find(query)
          .sort({ createdAt: -1 })
          .toArray();

        const complaintsWithDetails = await Promise.all(
          complaints.map(async (complaint) => {
            const form = await formsCollection.findOne({
              _id: complaint.formId,
            });
            let department = null;
            let position = null;

            if (complaint.departmentId) {
              department = await departmentsCollection.findOne({
                _id: new ObjectId(complaint.departmentId)
              });
            }
            if (complaint.positionId) {
              position = await positionsCollection.findOne({
                _id: new ObjectId(complaint.positionId)
              });
            }

            return {
              ...complaint,
              formName: form?.name || "Unknown Form",
              department: department?.name || null,
              position: position?.name || null,
            };
          })
        );

        res.send({ success: true, data: complaintsWithDetails });
      } catch (error) {
        console.error("Error fetching complaints:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.patch("/complaints/:id/status", async (req, res) => {
      try {
        const { id } = req.params;
        const { status, note, changedBy } = req.body;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid complaint ID" });
        }

        const historyEntry = {
          status,
          changedBy,
          changedAt: new Date(),
          note: note || `Status changed to ${status}`,
        };

        await complaintsCollection.updateOne(
          { _id: new ObjectId(id) },
          {
            $set: { status, updatedAt: new Date() },
            $push: { history: historyEntry },
          }
        );

        res.send({ success: true, message: "Status updated" });
      } catch (error) {
        console.error("Error updating status:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.patch("/complaints/:id/transfer", async (req, res) => {
      try {
        const { id } = req.params;
        const { assignedTo, assignedToEmail, note, changedBy } = req.body;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid complaint ID" });
        }

        const historyEntry = {
          status: "pending",
          assignedTo,
          changedBy,
          changedAt: new Date(),
          note: note || `Transferred to ${assignedToEmail}`,
        };

        await complaintsCollection.updateOne(
          { _id: new ObjectId(id) },
          {
            $set: { assignedTo, assignedToEmail, updatedAt: new Date() },
            $push: { history: historyEntry },
          }
        );

        res.send({ success: true, message: "Complaint transferred" });
      } catch (error) {
        console.error("Error transferring complaint:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.patch("/complaints/:id/department", async (req, res) => {
      try {
        const { id } = req.params;
        const { departmentId, positionId, note, changedBy } = req.body;

        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid complaint ID" });
        }

        const historyEntry = {
          departmentId: departmentId || null,
          positionId: positionId || null,
          changedBy,
          changedAt: new Date(),
          note: note || "Department/Position updated",
        };

        await complaintsCollection.updateOne(
          { _id: new ObjectId(id) },
          {
            $set: { departmentId: departmentId || null, positionId: positionId || null, updatedAt: new Date() },
            $push: { history: historyEntry },
          }
        );

        res.send({ success: true, message: "Department/Position updated" });
      } catch (error) {
        console.error("Error updating department:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    // ==================== PUBLIC ENDPOINTS ====================
    app.get("/public/organizations", async (req, res) => {
      try {
        const { search } = req.query;
        let query = { userRole: "organization" };

        if (search) {
          query.$or = [
            { organizationName: { $regex: search, $options: "i" } },
            { organizationUsername: { $regex: search, $options: "i" } },
          ];
        }

        const organizations = await userCollection
          .find(query)
          .project({ organizationName: 1, organizationUsername: 1, organizationLogo: 1 })
          .limit(50)
          .toArray();

        res.send({ success: true, data: organizations });
      } catch (error) {
        console.error("Error fetching organizations:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/public/organization/:username", async (req, res) => {
      try {
        const { username } = req.params;

        const organization = await userCollection.findOne(
          { userRole: "organization", organizationUsername: username },
          { projection: { organizationName: 1, organizationUsername: 1, organizationLogo: 1 } }
        );

        if (!organization) {
          return res.status(404).send({ message: "Organization not found" });
        }

        const forms = await formsCollection
          .find({ organizationUsername: username })
          .project({ name: 1, fields: 1 })
          .toArray();

        const departments = await departmentsCollection
          .find({ organizationUsername: username })
          .toArray();

        res.send({
          success: true,
          data: { ...organization, forms, departments },
        });
      } catch (error) {
        console.error("Error fetching organization:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    app.get("/public/form/:id", async (req, res) => {
      try {
        const { id } = req.params;
        if (!ObjectId.isValid(id)) {
          return res.status(400).send({ message: "Invalid form ID" });
        }

        const form = await formsCollection.findOne({ _id: new ObjectId(id) });
        if (!form) {
          return res.status(404).send({ message: "Form not found" });
        }

        const company = await userCollection.findOne(
          { organizationUsername: form.organizationUsername },
          { projection: { organizationName: 1, organizationUsername: 1, organizationLogo: 1 } }
        );

        const assignments = await formAssignmentsCollection
          .find({ formId: id })
          .toArray();

        let departments = [];
        let positions = [];

        if (assignments.length > 0) {
          const deptIds = [...new Set(assignments.map(a => a.departmentId).filter(Boolean))];
          if (deptIds.length > 0) {
            departments = await departmentsCollection
              .find({
                _id: { $in: deptIds.map(id => new ObjectId(id)) },
                organizationUsername: form.organizationUsername
              })
              .toArray();
          }

          const posIds = [...new Set(assignments.map(a => a.positionId).filter(Boolean))];
          if (posIds.length > 0) {
            positions = await positionsCollection
              .find({
                _id: { $in: posIds.map(id => new ObjectId(id)) },
                organizationUsername: form.organizationUsername
              })
              .toArray();
          }
        }

        res.send({
          success: true,
          data: {
            form,
            company,
            departments,
            positions,
            hasAssignments: assignments.length > 0,
          },
        });
      } catch (error) {
        console.error("Error fetching form:", error);
        res.status(500).send({ message: "Internal server error" });
      }
    });

    await client.db("admin").command({ ping: 1 });
    console.log("Successfully connected to MongoDB");

    // Start server with Socket.IO
    server.listen(port, () => {
      console.log(`Server is running on port ${port}`);
    });
  } catch (error) {
    console.error("Database connection error:", error);
  }
}

run().catch(console.error);

app.get("/", (req, res) => {
  res.send("Server is running");
});