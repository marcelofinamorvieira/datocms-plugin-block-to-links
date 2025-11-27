# DatoCMS Block to Links Plugin

A powerful migration tool for DatoCMS that automatically converts Modular Content Blocks into independent Models referenced via Links.

## 🚀 Why use this plugin?

As your DatoCMS project grows, you might find that some **Modular Blocks** would serve you better as independent **Models**. 

- **Reusability**: Blocks are embedded and unique to their parent. Models can be referenced from multiple places.
- **API Cleanliness**: deeply nested blocks can make API responses heavy. Links allow you to fetch data only when needed.
- **Content Management**: Editors can manage these records independently in the content tab.

Manually migrating data from Blocks to Models is tedious and error-prone. This plugin automates the entire process, preserving your content and relationships.

## ✨ Features

- **🔍 Smart Analysis**: Scans your project to find where a Block is used and how many records will be affected before you commit.
- **🛡️ Safe Migration Mode**: Option to create new models/links *alongside* your existing blocks without deleting anything, allowing for safe verification.
- **⚡️ Full Replacement Mode**: Option to fully migrate and automatically delete the old Block definition and data for a clean switch.
- **📦 Data Preservation**: Migrates all fields and values from the Block to the new Model.
- **🔄 Auto-Publish**: Optional setting to automatically publish the new records and updated parent records.

## 🛠 Installation

1. Go to your DatoCMS project settings.
2. Navigate to **Plugins**.
3. Click the **Plus** icon to install a new plugin.
4. Search for `Block To Links` or install manually using the entry point URL if you are self-hosting.

## ⚙️ Configuration

This plugin requires the **"Current user access token"** permission to perform schema changes and content updates on your behalf.

1. After installing, go to the plugin settings.
2. Ensure the plugin has the necessary permissions granted.

## 📖 How to Use

1. **Open the Plugin**: Navigate to the "Block to Links" plugin page in your DatoCMS dashboard.
2. **Select a Block**: Choose the Modular Block model you want to convert from the dropdown list.
3. **Analyze**: The plugin will analyze your content to show:
   - The structure of the block.
   - How many records contain this block.
   - Which fields references this block.
4. **Choose Conversion Options**:
   - **Fully replace original block**: 
     - ✅ **Enabled**: Deletes the original block and replaces it with a Link field. (Destructive)
     - ❌ **Disabled** (Recommended): Adds a new Link field alongside the existing Block field. You can switch manually later.
   - **Publish records after changes**: Automatically publishes the new records.
5. **Convert**: Click the button to start the migration. 
   - Watch the progress bar as the plugin creates the new Model, migrates content, and updates references.

## ⚠️ Important Notes

- **Backup First**: While this plugin is tested, data migration always carries risk. We strongly recommend performing a backup of your environment or testing on a sandbox environment first.
- **API Keys**: The new Model will have a generated API key based on the original Block's key.
- **Field Types**: The plugin attempts to map all Block fields to Model fields 1:1.

## 💻 Development

This project is built with React, Vite, and the DatoCMS Plugin SDK.

### Prerequisites

- Node.js (v18+)
- pnpm

### Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/marcelofinamorvieira/datocms-plugin-block-to-links.git
   ```
2. Install dependencies:
   ```bash
   pnpm install
   ```
3. Start the development server:
   ```bash
   pnpm dev
   ```

### Building for Production

To build the plugin for production deployment:

```bash
pnpm build
```

The output will be in the `dist` directory.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

MIT
