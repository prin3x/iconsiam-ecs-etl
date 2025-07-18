import { PrismaClient } from "@prisma/client";
import { Decimal } from "@prisma/client/runtime/library";
import * as dotenv from "dotenv";
import fetch from "node-fetch";
import "./src/database.ts";

// Load environment variables
dotenv.config();

const prisma = new PrismaClient();

interface ExternalApiRecord {
  uniqueId: string;
  tenantId: string;
  sourceSystem: string;
  type: string;
  recordTypeName: string;
  bandCode: string;
  brandNameEn: string;
  brandNameTh: string;
  buildingName: string;
  buildingCode: string;
  shopNameEnglish: string;
  shopNameThai: string;
  status: boolean;
  statusRevised: string;
  categoryNameEn: string;
  categoryNameTh: string;
  subCategoryEn: string;
  subCategoryTh: string;
  zone: string;
  floor: string;
  floorRevised: string;
  openingHours: string;
  lastOrder: string;
  unit: string;
  tel: string;
  contact: string;
  website: string;
  faqs: string;
  descriptionEn: string;
  descriptionTh: string;
  attribute1: string;
  attribute2: string;
  attribute3Gender3: string;
  attribute4Gender4: string;
  attribute5ProductOffering1: string;
  attribute6ProductOffering2: string;
  attribute7ProductOffering3: string;
  attribute8ProductOffering4: string;
  attribute9ProductOffering5: string;
  attribute10: string;
  attribute11: string;
  attribute12: string;
  attribute13: string;
  attribute14: string;
  attribute15: string;
  attribute16: string;
  attribute17: string;
  attribute18: string;
  attribute19: string;
  attribute20: string;
  attribute21: string;
  attribute22: string;
  attribute23: string;
  attribute24: string;
  attribute25: string;
  attribute26: string;
  attribute27: string;
  attribute28: string;
  attribute29: string;
  attribute30: string;
  attribute31: string;
  attribute32: string;
  attribute33: string;
  attribute34: string;
  attribute35: string;
  attribute36: string;
  attribute37: string;
  attribute38: string;
  attribute39: string;
  logo: string;
  gallery_1: string;
  gallery_2: string;
  gallery_3: string;
  gallery_4: string;
  mapDirectory: string;
  oneSiamTenantOnboard: string;
  siamGiftCard: string;
  changeRequest: string;
  changeReason: string;
  startDate: string;
  endDate: string;
  tenantLastUpdate: string;
  mallLastUpdate: string | null;
  listId: string;
  action: string;
  jobSyncDate: string;
}

interface ExternalApiResponse {
  totalRecord: number;
  page: number;
  limit: number;
  totalPages: number;
  data: ExternalApiRecord[];
}

// Utility to decode HTML entities and unicode escapes
function decodeText(text: string | null | undefined): string {
  if (!text) return "";
  const htmlEntities: { [key: string]: string } = {
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&#039;": "'",
    "&apos;": "'",
    "&nbsp;": " ",
  };

  let decoded = text;
  const entityRegex = /&[a-zA-Z0-9#]+;/g;
  let prev;
  do {
    prev = decoded;
    decoded = prev.replace(
      entityRegex,
      (entity) => htmlEntities[entity] || entity
    );
  } while (prev !== decoded);

  decoded = decoded.replace(/&#(\d+);/g, (_, code) =>
    String.fromCharCode(Number(code))
  );
  decoded = decoded.replace(/\\u([0-9a-fA-F]{4})/g, (_, code) =>
    String.fromCharCode(parseInt(code, 16))
  );
  return decoded;
}

// Generate unique sync ID
function generateSyncId(): string {
  return `sync_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Track unmapped floor names
const unmappedFloors = new Set<string>();

// Track unmapped category names
const unmappedCategories = new Set<string>();

// Track validation issues
const validationIssues: Array<{
  record_unique_id: string;
  description: string;
}> = [];

// Track errors
const errors: Array<{
  timestamp: Date;
  record_unique_id: string;
  error_message: string;
  error_stack: string;
}> = [];

// Performance tracking
let totalApiResponseTime = 0;
let apiRequestCount = 0;
let totalProcessingTime = 0;
let recordProcessingCount = 0;

// Fetch data from external API
async function fetchExternalData(
  apiUrl: string,
  apiKey?: string
): Promise<ExternalApiRecord[]> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  if (process.env.NODE_ENV === "production") {
    headers["Cookie"] = process.env.EXTERNAL_API_COOKIES || "";
  }

  if (apiKey) {
    headers["X-Apig-AppCode"] = `${apiKey}`;
  }

  const allRecords: ExternalApiRecord[] = [];
  let currentPage = 1;
  let totalPages = 1;

  do {
    try {
      console.log(`Fetching page ${currentPage}...`);
      const startTime = Date.now();

      const response = await fetch(`${apiUrl}?page=${currentPage}&limit=100`, {
        method: "GET",
        headers,
      });

      const endTime = Date.now();
      const responseTime = endTime - startTime;
      totalApiResponseTime += responseTime;
      apiRequestCount++;

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = (await response.json()) as ExternalApiResponse;

      allRecords.push(...data.data);
      totalPages = data.totalPages;
      currentPage++;

      console.log(
        `Fetched ${data.data.length} records from page ${
          currentPage - 1
        } (${responseTime}ms)`
      );

      // Add a small delay to be respectful to the API
      await new Promise((resolve) => setTimeout(resolve, 100));
    } catch (error) {
      console.error(`Error fetching page ${currentPage}:`, error);
      errors.push({
        timestamp: new Date(),
        record_unique_id: `page_${currentPage}`,
        error_message: error instanceof Error ? error.message : "Unknown error",
        error_stack: error instanceof Error ? error.stack || "" : "",
      });
      break;
    }
  } while (currentPage <= totalPages);

  console.log(`Total records fetched: ${allRecords.length}`);
  return allRecords;
}

// Find existing floor
async function findFloor(floorName: string): Promise<string | null> {
  if (!floorName) return null;

  // Floor mappings based on your existing logic
  const floorMappings: { [key: string]: string } = {
    // Direct matches
    B1: "B1",
    B2: "B2",
    GF: "GF",
    UG: "UG",
    MF: "MF",
    "1F": "1F",
    "2F": "2F",
    "3F": "3F",
    "4F": "4F",
    "5F": "5F",
    "6F": "6F",
    "7F": "7F",
    "8F": "8F",
    "1f": "1F",
    "basement 1": "B1",
    "basement 2": "B2",
    "ground floor": "GF",
    "upper ground": "UG",
    "mezzanine floor": "MF",
    mezzanine: "MF",
    "first floor": "1F",
    "second floor": "2F",
    "third floor": "3F",
    "fourth floor": "4F",
    "fifth floor": "5F",
    "sixth floor": "6F",
    "seventh floor": "7F",
    "eighth floor": "8F",
    M: "MF",
    G: "GF",
    BM1: "B1",
    BM2: "B2",
    "1": "1F",
    "2": "2F",
    "3": "3F",
    "4": "4F",
    "5": "5F",
    "6": "6F",
    "7": "7F",
    "8": "8F",
    "B1 Floor": "B1",
    "B2 Floor": "B2",
    "GF Floor": "GF",
    "UG Floor": "UG",
    "MF Floor": "MF",
    "1F Floor": "1F",
    "2F Floor": "2F",
    "3F Floor": "3F",
    "4F Floor": "4F",
    "5F Floor": "5F",
    "6F Floor": "6F",
    "7F Floor": "7F",
    "8F Floor": "8F",
    "Fl. 1": "1F",
    "Fl. 2": "2F",
    "Fl. 3": "3F",
    "Fl. 4": "4F",
    "Fl. 5": "5F",
    "Fl. 6": "6F",
    "Fl. 7": "7F",
    "Fl. G": "GF",
    "Fl. M": "MF",
    "Fl. U": "UG",
    "Fl. B1": "B1",
    "Fl. B2": "B2",
    ",1": "1F",
    "2,3": "2F",
    "7,7A,8A": "7F",
    "7A": "7F",
    "7,8": "7F",
    BM: "BM",
    "Fl. BM1": "BM1",
    "Fl. BM1,G": "BM1",
    "Fl. GA": "GF",
    "Fl. BF": "BF",
    GA: "GF",
  };

  try {
    // Try exact match first
    let existingFloor = await prisma.floors.findFirst({
      where: {
        name: {
          equals: floorName,
          mode: "insensitive",
        },
      },
    });

    if (existingFloor) {
      console.log(`✅ Found existing floor: ${floorName}`);
      return existingFloor.id?.toString();
    }

    // Try mapped floor name (case-insensitive)
    const mappedFloorName =
      floorMappings[floorName.toLowerCase()] ||
      Object.entries(floorMappings).find(
        ([key]) => key.toLowerCase() === floorName.toLowerCase()
      )?.[1];
    if (mappedFloorName) {
      existingFloor = await prisma.floors.findFirst({
        where: {
          name: {
            equals: mappedFloorName,
            mode: "insensitive",
          },
        },
      });

      if (existingFloor) {
        console.log(
          `✅ Found existing floor (mapped): ${floorName} → ${mappedFloorName}`
        );
        return existingFloor.id?.toString();
      }
    }

    // Try partial match
    existingFloor = await prisma.floors.findFirst({
      where: {
        name: {
          contains: floorName,
          mode: "insensitive",
        },
      },
    });

    if (existingFloor) {
      console.log(`✅ Found existing floor (partial match): ${floorName}`);
      return existingFloor.id?.toString();
    }

    console.log(`⚠️  Floor "${floorName}" not found`);
    unmappedFloors.add(floorName);
    return null;
  } catch (error) {
    console.error(`❌ Error finding floor ${floorName}:`, error);
    return null;
  }
}

// Find existing category
async function findCategory(
  categoryNameEn: string,
  categoryNameTh: string,
  type: "shops" | "dinings"
): Promise<string | null> {
  if (!categoryNameEn && !categoryNameTh) return null;

  const searchName = categoryNameEn || categoryNameTh;

  // Category mappings
  const categoryMappings: { [key: string]: string } = {
    "international luxury": "LUXURY",
    "international luxury brands": "LUXURY",
    "luxury international": "LUXURY",
    "premium luxury": "LUXURY",
    "high-end luxury": "LUXURY",
    "luxury brands": "LUXURY",
    "luxury fashion": "LUXURY",
    "fashion & accessories": "FASHION",
    "fashion&accessories": "FASHION",
    "fashion accessories": "FASHION",
    "international fashion": "FASHION",
    "premium fashion": "FASHION",
    "high-end fashion": "FASHION",
    "fashion brands": "FASHION",
    "health & beauty": "BEAUTY",
    "health beauty": "BEAUTY",
    "health&beauty": "BEAUTY",
    "beauty & wellness": "BEAUTY",
    "international beauty": "BEAUTY",
    "premium beauty": "BEAUTY",
    "beauty brands": "BEAUTY",
    "international cosmetics": "BEAUTY",
    "premium cosmetics": "BEAUTY",
    "mobile, gadget, electronics": "GADGET",
    "mobile gadget electronics": "GADGET",
    "electronics & gadgets": "GADGET",
    "gadget electronics": "GADGET",
    "mobile electronics": "GADGET",
    "food & beverage": "RESTAURANT",
    "food beverage": "RESTAURANT",
    "food and beverage": "RESTAURANT",
    "grocery, lifestyle & department store": "HOME & LIVING",
    "grocery lifestyle department store": "HOME & LIVING",
    "lifestyle department store": "HOME & LIVING",
    "grocery lifestyle": "HOME & LIVING",
    "leisure and entertainment": "CLUB & LOUNGE",
    "leisure entertainment": "CLUB & LOUNGE",
    "entertainment leisure": "CLUB & LOUNGE",
    service: "GENERAL",
    services: "GENERAL",
    specialty: "GENERAL",
    "specialty items": "COSMETIC & FRAGRANCE",
    "cosmetic & fragrance": "COSMETIC & FRAGRANCE",
    "cosmetic fragrance": "COSMETIC & FRAGRANCE",
    "cosmetic & fragrances": "COSMETIC & FRAGRANCE",
    "cosmetic fragrances": "COSMETIC & FRAGRANCE",
    "cosmetics & fragrance": "COSMETIC & FRAGRANCE",
    "cosmetics fragrance": "COSMETIC & FRAGRANCE",
  };

  try {
    // Try exact match first
    let existingCategory = await prisma.categories.findFirst({
      where: {
        categories_locales: {
          some: {
            locale: "en",
            name: {
              equals: searchName,
            },
          },
        },
        type: {
          equals: type,
        },
      },
    });

    if (existingCategory) {
      console.log(
        `✅ Found existing category (exact match): ${searchName} (${type})`
      );
      return existingCategory.id?.toString();
    }

    // Try category mappings
    const normalizedSearchName = searchName.toLowerCase().trim();
    const mappedCategory = categoryMappings[normalizedSearchName];

    if (mappedCategory) {
      existingCategory = await prisma.categories.findFirst({
        where: {
          categories_locales: {
            some: {
              locale: "en",
              name: {
                equals: mappedCategory,
                mode: "insensitive",
              },
            },
          },
          type: {
            equals: type,
          },
        },
      });

      if (existingCategory) {
        console.log(
          `✅ Found existing category (mapped): ${searchName} → ${mappedCategory} (${type})`
        );
        return existingCategory.id?.toString();
      }
    }

    // Try partial match
    existingCategory = await prisma.categories.findFirst({
      where: {
        categories_locales: {
          some: {
            locale: "en",
            name: {
              contains: searchName,
              mode: "insensitive",
            },
          },
        },
        type: {
          equals: type,
        },
      },
    });

    if (existingCategory) {
      console.log(
        `✅ Found existing category (partial match): ${searchName} (${type})`
      );
      return existingCategory.id?.toString();
    }

    console.log(`⚠️  Category "${searchName}" (${type}) does not exist`);
    unmappedCategories.add(
      JSON.stringify({
        category_name: searchName,
        type: type,
        english_name: categoryNameEn,
        thai_name: categoryNameTh,
      })
    );
    return null;
  } catch (error) {
    console.error(`❌ Error finding category ${searchName}:`, error);
    return null;
  }
}

// Parse opening hours
function parseOpeningHours(openingHours: string): any {
  if (!openingHours || openingHours.trim() === "") {
    return {
      same_hours_every_day: true,
      open: "",
      close: "",
      per_day: [],
    };
  }

  // Handle both dot and colon time formats
  const timeMatch = openingHours.match(
    /^(\d{1,2}[.:]\d{2})\s*-\s*(\d{1,2}[.:]\d{2})$/
  );
  if (timeMatch && timeMatch[1] && timeMatch[2]) {
    const openTime = timeMatch[1].replace(/[.:]/, ":");
    const closeTime = timeMatch[2].replace(/[.:]/, ":");

    return {
      same_hours_every_day: true,
      open: openTime,
      close: closeTime,
      per_day: [],
    };
  }

  return {
    same_hours_every_day: true,
    open: openingHours,
    close: "",
    per_day: [],
  };
}

// Determine if record is dining or shop
function determineRecordType(record: ExternalApiRecord): "dinings" | "shops" {
  const categoryName = record.categoryNameEn?.toLowerCase() || "";

  if (
    categoryName.includes("food") ||
    categoryName.includes("beverage") ||
    categoryName.includes("restaurant") ||
    categoryName.includes("cafe") ||
    categoryName.includes("bar") ||
    categoryName.includes("take home")
  ) {
    return "dinings";
  }

  return "shops";
}

// Generate safe slug
function generateSafeSlug(brandName: string, tenantId: string): string {
  if (!brandName || !tenantId) return "";

  return (
    brandName
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, "")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "") + `-${tenantId}`
  );
}

// Sync a single record
async function syncRecord(record: ExternalApiRecord) {
  const startTime = Date.now();

  try {
    const recordType = determineRecordType(record);
    const collection = recordType === "dinings" ? "dinings" : "shops";

    console.log(
      `Processing ${recordType}: ${
        record.brandNameEn || record.shopNameEnglish
      }`
    );

    const validationIssues: Array<{
      record_unique_id: string;
      description: string;
    }> = [];

    if (!record.uniqueId) {
      validationIssues.push({
        record_unique_id: "unknown",
        description: "Missing unique ID",
      });
    }

    if (
      !record.brandNameEn &&
      !record.shopNameEnglish &&
      !record.brandNameTh &&
      !record.shopNameThai
    ) {
      validationIssues.push({
        record_unique_id: "unknown",
        description: "No name found in any language",
      });
    }

    if (!record.tenantId) {
      validationIssues.push({
        record_unique_id: "unknown",
        description: "Missing tenant ID",
      });
    }

    // Track validation issues
    if (validationIssues.length > 0) {
      validationIssues.forEach((issue) => {
        validationIssues.push({
          record_unique_id: record.uniqueId || "unknown",
          description: issue.description,
        });
      });
    }

    // Find existing floor
    const floorName = record.floorRevised?.trim() || record.floor?.trim();
    console.log(`   🏢 Floor name from API: "${floorName}"`);
    const floorId = await findFloor(floorName);

    // Find existing category
    const categoryId = await findCategory(
      record.categoryNameEn?.toLowerCase()?.trim(),
      record.categoryNameTh?.toLowerCase()?.trim(),
      recordType
    );

    // Parse dates
    const parseDate = (dateStr: string) => {
      if (!dateStr || dateStr.trim() === "") return null;
      const date = new Date(dateStr);
      return isNaN(date.getTime()) ? null : date.toISOString().split("T")[0];
    };

    // Parse opening hours from the record
    const parsedOpeningHours = parseOpeningHours(record.openingHours);

    // Create base data with type assertion (without status - will be set differently for new vs existing)
    const baseData = {
      slug: "",
      unique_id: "",
      location_zone: record.zone || null,
      opening_hours_same_hours_every_day:
        parsedOpeningHours.same_hours_every_day,
      opening_hours_open: parsedOpeningHours.open,
      opening_hours_close: parsedOpeningHours.close,
      short_alphabet: "",
      floors: floorId ? { connect: { id: parseInt(floorId) } } : undefined,
      is_featured: false,
      sort_order: new Decimal(0),
      pin_to_iconluxe: false,
    } as any;

    // Add category if found - we'll handle this separately after create/update
    let categoryToConnect = categoryId;
    if (categoryId) {
      console.log(
        `   📂 Category assigned: ${record.categoryNameEn} → Category ID: ${categoryId}`
      );
    }

    // Prepare locale data
    const localeData = {
      en: {
        title:
          record.brandNameEn || record.shopNameEnglish
            ? decodeText(record.brandNameEn || record.shopNameEnglish)
            : null,
        subtitle: record.shopNameThai ? decodeText(record.shopNameThai) : null,
        description: record.descriptionEn
          ? decodeText(record.descriptionEn)
          : null,
        meta_title: record.brandNameEn ? decodeText(record.brandNameEn) : null,
        meta_description: record.descriptionEn
          ? decodeText(record.descriptionEn)
          : null,
      },
      th: {
        title:
          record.brandNameTh || record.shopNameThai
            ? decodeText(record.brandNameTh || record.shopNameThai)
            : null,
        subtitle: record.shopNameEnglish
          ? decodeText(record.shopNameEnglish)
          : null,
        description: record.descriptionTh
          ? decodeText(record.descriptionTh)
          : null,
        meta_title: record.brandNameTh ? decodeText(record.brandNameTh) : null,
        meta_description: record.descriptionTh
          ? decodeText(record.descriptionTh)
          : null,
      },
      zh: {
        title:
          record.brandNameEn || record.shopNameEnglish
            ? decodeText(record.brandNameEn || record.shopNameEnglish)
            : null,
        subtitle: record.shopNameThai ? decodeText(record.shopNameThai) : null,
        description: record.descriptionEn
          ? decodeText(record.descriptionEn)
          : null,
        meta_title: record.brandNameEn ? decodeText(record.brandNameEn) : null,
        meta_description: record.descriptionEn
          ? decodeText(record.descriptionEn)
          : null,
      },
    };

    // Always add unique_id
    if (record.uniqueId) {
      baseData.unique_id = record.uniqueId;
      console.log(`   🆔 Unique ID assigned: ${record.uniqueId}`);
    }

    // Check if record already exists
    let existingRecord: any = null;

    if (record.uniqueId) {
      if (recordType === "dinings") {
        existingRecord = await prisma.dinings.findFirst({
          where: {
            unique_id: record.uniqueId,
          },
          include: {
            floors: true,
          },
        });
      } else {
        existingRecord = await prisma.shops.findFirst({
          where: {
            unique_id: record.uniqueId,
          },
          include: {
            floors: true,
          },
        });
      }
    }

    if (existingRecord) {
      // Update existing record
      console.log(
        `🔄 Updating existing ${recordType}: ${
          record.brandNameEn || record.shopNameEnglish
        }`
      );

      // Handle categories update - we'll handle this separately after the update
      let categoryToConnect = categoryId;

      if (recordType === "dinings") {
        await prisma.dinings.update({
          where: {
            id: existingRecord.id,
          },
          data: baseData,
        });
      } else {
        await prisma.shops.update({
          where: {
            id: existingRecord.id,
          },
          data: baseData,
        });
      }

      // Handle category connection for existing record - only add if not already linked
      if (categoryToConnect) {
        if (recordType === "dinings") {
          // Check if this relationship already exists
          const existingRel = await prisma.dinings_rels.findFirst({
            where: {
              parent_id: existingRecord.id,
              categories_id: parseInt(categoryToConnect),
            },
          });

          if (!existingRel) {
            await prisma.dinings_rels.create({
              data: {
                parent_id: existingRecord.id,
                categories_id: parseInt(categoryToConnect),
                path: "/",
                order: 0,
              },
            });
            console.log(
              `   📂 Added category relationship for existing dining`
            );
          } else {
            console.log(
              `   📂 Category relationship already exists for dining`
            );
          }
        } else {
          // Check if this relationship already exists
          const existingRel = await prisma.shops_rels.findFirst({
            where: {
              parent_id: existingRecord.id,
              categories_id: parseInt(categoryToConnect),
            },
          });

          if (!existingRel) {
            await prisma.shops_rels.create({
              data: {
                parent_id: existingRecord.id,
                categories_id: parseInt(categoryToConnect),
                path: "/",
                order: 0,
              },
            });
            console.log(`   📂 Added category relationship for existing shop`);
          } else {
            console.log(`   📂 Category relationship already exists for shop`);
          }
        }
      }

      // Update locale entries for existing record
      const locales = ["en", "th", "zh"] as const;
      for (const locale of locales) {
        const localeInfo = localeData[locale];
        if (localeInfo.title || localeInfo.subtitle || localeInfo.description) {
          if (recordType === "dinings") {
            // Upsert locale entry
            await prisma.dinings_locales.upsert({
              where: {
                locale_parent_id: {
                  locale: locale,
                  parent_id: existingRecord.id,
                },
              },
              update: {
                title: localeInfo.title,
                subtitle: localeInfo.subtitle,
                description: localeInfo.description,
                meta_title: localeInfo.meta_title,
                meta_description: localeInfo.meta_description,
              },
              create: {
                parent_id: existingRecord.id,
                locale: locale,
                title: localeInfo.title,
                subtitle: localeInfo.subtitle,
                description: localeInfo.description,
                meta_title: localeInfo.meta_title,
                meta_description: localeInfo.meta_description,
              },
            });
          } else {
            // Upsert locale entry
            await prisma.shops_locales.upsert({
              where: {
                locale_parent_id: {
                  locale: locale,
                  parent_id: existingRecord.id,
                },
              },
              update: {
                title: localeInfo.title,
                subtitle: localeInfo.subtitle,
                description: localeInfo.description,
                meta_title: localeInfo.meta_title,
                meta_description: localeInfo.meta_description,
              },
              create: {
                parent_id: existingRecord.id,
                locale: locale,
                title: localeInfo.title,
                subtitle: localeInfo.subtitle,
                description: localeInfo.description,
                meta_title: localeInfo.meta_title,
                meta_description: localeInfo.meta_description,
              },
            });
          }
        }
      }

      console.log(
        `✅ Updated ${recordType}: ${
          record.brandNameEn || record.shopNameEnglish
        }`
      );
    } else {
      // Create new record
      const newSlug =
        record.brandNameEn || record.shopNameEnglish
          ? generateSafeSlug(
              record.brandNameEn || record.shopNameEnglish,
              record.tenantId
            )
          : `shop-${record.tenantId}`;

      const createData = {
        ...baseData,
        slug: newSlug,
        status: "INACTIVE" as const,
      } as any;

      let createdRecord;
      if (recordType === "dinings") {
        createdRecord = await prisma.dinings.create({
          data: createData,
        });
      } else {
        createdRecord = await prisma.shops.create({
          data: createData,
        });
      }

      // Handle category connection for new record
      if (categoryToConnect) {
        if (recordType === "dinings") {
          await prisma.dinings_rels.create({
            data: {
              parent_id: createdRecord.id,
              categories_id: categoryToConnect
                ? parseInt(categoryToConnect)
                : null,
              path: "/",
              order: 0,
            },
          });
          console.log(`   📂 Added category relationship for new dining`);
        } else {
          await prisma.shops_rels.create({
            data: {
              parent_id: createdRecord.id,
              categories_id: categoryToConnect
                ? parseInt(categoryToConnect)
                : null,
              path: "/",
              order: 0,
            },
          });
          console.log(`   📂 Added category relationship for new shop`);
        }
      }

      // Create locale entries for new record
      const locales = ["en", "th", "zh"] as const;
      for (const locale of locales) {
        const localeInfo = localeData[locale];
        if (recordType === "dinings") {
          await prisma.dinings_locales.create({
            data: {
              parent_id: createdRecord.id,
              locale: locale,
              title: localeInfo.title,
              subtitle: localeInfo.subtitle,
              description: localeInfo.description,
              meta_title: localeInfo.meta_title,
              meta_description: localeInfo.meta_description,
            },
          });
        } else {
          await prisma.shops_locales.create({
            data: {
              parent_id: createdRecord.id,
              locale: locale,
              title: localeInfo.title,
              subtitle: localeInfo.subtitle,
              description: localeInfo.description,
              meta_title: localeInfo.meta_title,
              meta_description: localeInfo.meta_description,
            },
          });
        }
      }

      console.log(
        `✅ Created ${recordType}: ${
          record.brandNameEn || record.shopNameEnglish
        }`
      );
      console.log(`   Created new slug: ${newSlug}`);
      console.log(`   Status set to: INACTIVE`);
    }

    const endTime = Date.now();
    const processingTime = endTime - startTime;
    totalProcessingTime += processingTime;
    recordProcessingCount++;

    return { validationIssues, recordType, wasCreated: !existingRecord };
  } catch (error) {
    const endTime = Date.now();
    const processingTime = endTime - startTime;
    totalProcessingTime += processingTime;
    recordProcessingCount++;

    console.error(`❌ Error syncing record ${record.uniqueId}:`, error);

    errors.push({
      timestamp: new Date(),
      record_unique_id: record.uniqueId || "unknown",
      error_message: error instanceof Error ? error.message : "Unknown error",
      error_stack: error instanceof Error ? error.stack || "" : "",
    });

    throw error;
  }
}

// Get mapped floor function (equivalent to the original getMappedFloor)
async function getMappedFloor(
  floorName: string
): Promise<{ id: string; name: string; code?: string } | null> {
  if (!floorName) return null;

  // Floor mappings based on your existing logic
  const floorMappings: { [key: string]: string } = {
    // Direct matches
    B1: "B1",
    B2: "B2",
    GF: "GF",
    UG: "UG",
    MF: "MF",
    "1F": "1F",
    "2F": "2F",
    "3F": "3F",
    "4F": "4F",
    "5F": "5F",
    "6F": "6F",
    "7F": "7F",
    "8F": "8F",
    "1f": "1F",
    "basement 1": "B1",
    "basement 2": "B2",
    "ground floor": "GF",
    "upper ground": "UG",
    "mezzanine floor": "MF",
    mezzanine: "MF",
    "first floor": "1F",
    "second floor": "2F",
    "third floor": "3F",
    "fourth floor": "4F",
    "fifth floor": "5F",
    "sixth floor": "6F",
    "seventh floor": "7F",
    "eighth floor": "8F",
    M: "MF",
    G: "GF",
    BM1: "B1",
    BM2: "B2",
    "1": "1F",
    "2": "2F",
    "3": "3F",
    "4": "4F",
    "5": "5F",
    "6": "6F",
    "7": "7F",
    "8": "8F",
    "B1 Floor": "B1",
    "B2 Floor": "B2",
    "GF Floor": "GF",
    "UG Floor": "UG",
    "MF Floor": "MF",
    "1F Floor": "1F",
    "2F Floor": "2F",
    "3F Floor": "3F",
    "4F Floor": "4F",
    "5F Floor": "5F",
    "6F Floor": "6F",
    "7F Floor": "7F",
    "8F Floor": "8F",
    "Fl. 1": "1F",
    "Fl. 2": "2F",
    "Fl. 3": "3F",
    "Fl. 4": "4F",
    "Fl. 5": "5F",
    "Fl. 6": "6F",
    "Fl. 7": "7F",
    "Fl. G": "GF",
    "Fl. M": "MF",
    "Fl. U": "UG",
    "Fl. B1": "B1",
    "Fl. B2": "B2",
    ",1": "1F",
    "2,3": "2F",
    "7,7A,8A": "7F",
    BM: "BM",
    "Fl. BM1": "BM1",
    "Fl. BM1,G": "BM1",
    "Fl. GA": "GF",
    "Fl. BF": "BF",
    GA: "GF",
  };

  try {
    // Try exact match first
    let existingFloor = await prisma.floors.findFirst({
      where: {
        name: {
          equals: floorName,
        },
      },
    });

    if (existingFloor) {
      return {
        id: existingFloor.id.toString(),
        name: existingFloor.name,
        code: existingFloor.name, // Use name as code for now
      };
    }

    // Try mapped floor name (case-insensitive)
    const mappedFloorName =
      floorMappings[floorName.toLowerCase()] ||
      Object.entries(floorMappings).find(
        ([key]) => key.toLowerCase() === floorName.toLowerCase()
      )?.[1];
    if (mappedFloorName) {
      existingFloor = await prisma.floors.findFirst({
        where: {
          name: {
            equals: mappedFloorName,
          },
        },
      });

      if (existingFloor) {
        return {
          id: existingFloor.id.toString(),
          name: existingFloor.name,
          code: existingFloor.name,
        };
      }
    }

    // Try partial match
    existingFloor = await prisma.floors.findFirst({
      where: {
        name: {
          contains: floorName,
        },
      },
    });

    if (existingFloor) {
      return {
        id: existingFloor.id.toString(),
        name: existingFloor.name,
        code: existingFloor.name,
      };
    }

    console.log(`⚠️  Floor "${floorName}" not found`);
    unmappedFloors.add(floorName);
    return null;
  } catch (error) {
    console.error(`❌ Error finding floor ${floorName}:`, error);
    return null;
  }
}

// Main sync function
async function syncExternalApi() {
  const syncId = generateSyncId();
  let syncLog: any = null;

  try {
    console.log("=== Starting External API Sync ===");
    console.log(`Sync ID: ${syncId}`);

    // Configuration
    const API_URL =
      process.env.EXTERNAL_API_URL ||
      "https://qa-api-internal.onesiam.com/spwdirectoryservice/v1/Directories/Icon/TenantMalls";
    const API_KEY = process.env.X_APIG_APPCODE;

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (process.env.NODE_ENV === "production") {
      headers["Cookie"] = process.env.EXTERNAL_API_COOKIES || "";
    }

    if (!API_URL) {
      throw new Error("EXTERNAL_API_URL environment variable is required");
    }

    // Create sync log entry
    syncLog = await prisma.api_sync_logs.create({
      data: {
        sync_id: syncId,
        status: "RUNNING" as const,
        external_api_url: API_URL,
        started_at: new Date(),
        api_response_summary_total_records_fetched: 0,
        api_response_summary_total_pages: 0,
        api_response_summary_last_page_processed: 0,
        processing_summary_records_processed: 0,
        processing_summary_records_created: 0,
        processing_summary_records_updated: 0,
        processing_summary_records_failed: 0,
        processing_summary_shops_processed: 0,
        processing_summary_dinings_processed: 0,
      },
    });

    // Fetch data from external API
    const records = await fetchExternalData(API_URL, API_KEY);

    if (records.length === 0) {
      console.log("No records found from external API");
      await prisma.api_sync_logs.update({
        where: { id: syncLog.id },
        data: {
          status: "COMPLETED",
          completed_at: new Date(),
          api_response_summary_total_records_fetched: 0,
          api_response_summary_total_pages: 0,
          api_response_summary_last_page_processed: 0,
          processing_summary_records_processed: 0,
          processing_summary_records_created: 0,
          processing_summary_records_updated: 0,
          processing_summary_records_failed: 0,
          processing_summary_shops_processed: 0,
          processing_summary_dinings_processed: 0,
        },
      });
      return;
    }

    console.log(`📊 Processing ${records.length} records in batches...`);

    let successCount = 0;
    let errorCount = 0;
    let createdCount = 0;
    let updatedCount = 0;
    let shopsProcessed = 0;
    let diningsProcessed = 0;

    // Process records in batches
    const batchSize = parseInt(process.env.SYNC_BATCH_SIZE || "10");
    const batches: ExternalApiRecord[][] = [];

    for (let i = 0; i < records.length; i += batchSize) {
      batches.push(records.slice(i, i + batchSize));
    }

    console.log(
      `🔄 Processing ${batches.length} batches of ${batchSize} records each`
    );

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      console.log(
        `\n📦 Processing batch ${batchIndex + 1}/${batches.length} (${
          batch.length
        } records)`
      );

      // Process batch in parallel
      const batchPromises = batch.map(async (record) => {
        try {
          // --- MULTI-FLOOR SPLIT LOGIC ---
          if (typeof record.floor === "string" && record.floor.includes(",")) {
            const floorNames = record.floor
              .split(",")
              .map((f) => f && f.trim())
              .filter((f) => !!f);
            // Map each floor to DB floor
            const mappedFloors = await Promise.all(
              floorNames.map((f) => (f ? getMappedFloor(f) : null))
            );
            // Deduplicate by mapped floor id
            const uniqueFloors = Array.from(
              new Map(
                mappedFloors.filter((f) => f && f.id).map((f) => [f!.id, f!])
              ).values()
            );
            if (uniqueFloors.length === 1) {
              // Only one unique mapped floor, sync as usual
              record.floor = uniqueFloors[0].name;
              record.floorRevised = uniqueFloors[0].name;
              const result = await syncRecord(record);
              return {
                success: true,
                record,
                validationIssues: result?.validationIssues || [],
                recordType: result?.recordType,
                error: undefined,
              };
            } else {
              // Multiple unique mapped floors, clone and sync for each
              const results = await Promise.all(
                uniqueFloors.map(async (floorObj) => {
                  if (!floorObj)
                    return {
                      success: false,
                      record,
                      validationIssues: [],
                      recordType: null,
                      error: "Null floorObj",
                    };
                  const cloned = { ...record };
                  cloned.floor = floorObj.name;
                  cloned.floorRevised = floorObj.name;
                  // Append -FLOORCODE to uniqueId
                  const floorCode =
                    floorObj?.code ?? floorObj.name ?? floorObj.id;
                  cloned.uniqueId = `${record.uniqueId}-${floorCode}`;
                  const result = await syncRecord(cloned);
                  return {
                    success: true,
                    record: cloned,
                    validationIssues: result?.validationIssues || [],
                    recordType: result?.recordType,
                    wasCreated: result?.wasCreated || false,
                    error: undefined,
                  };
                })
              );
              return results[0]; // just return first for batch count
            }
          } else {
            // Not a multi-floor record, sync as usual
            const result = await syncRecord(record);
            return {
              success: true,
              record,
              validationIssues: result?.validationIssues || [],
              recordType: result?.recordType,
              wasCreated: result?.wasCreated || false,
              error: undefined,
            };
          }
        } catch (error) {
          return {
            success: false,
            record,
            validationIssues: [],
            recordType: null,
            wasCreated: false,
            error,
          };
        }
      });

      const batchResults = await Promise.all(batchPromises);

      // Count results
      batchResults.forEach(async (result) => {
        if (result && result.success) {
          successCount++;

          // Count by record type
          if (result.recordType === "shops") {
            shopsProcessed++;
          } else if (result.recordType === "dinings") {
            diningsProcessed++;
          }

          // Determine if record was created or updated based on sync result
          if (result.wasCreated) {
            createdCount++;
          } else {
            updatedCount++;
          }
        } else {
          errorCount++;
          if (result && result.record) {
            console.error(
              `Error processing record ${result.record.uniqueId}:`,
              result.error
            );
          }
        }
      });

      console.log(
        `✅ Batch ${batchIndex + 1} completed: ${
          batchResults.filter((r) => r.success).length
        }/${batch.length} successful`
      );
    }

    // Calculate performance metrics
    const avgTimePerRecord =
      recordProcessingCount > 0
        ? Math.round(totalProcessingTime / recordProcessingCount)
        : 0;
    const avgApiResponseTime =
      apiRequestCount > 0
        ? Math.round(totalApiResponseTime / apiRequestCount)
        : 0;
    const memoryUsage = process.memoryUsage().heapUsed / 1024 / 1024; // MB

    await prisma.api_sync_logs.update({
      where: { id: syncLog.id },
      data: {
        status: errorCount > 0 ? "PARTIAL" : "COMPLETED",
        completed_at: new Date(),
        api_response_summary_total_records_fetched: records.length,
        api_response_summary_total_pages: Math.ceil(records.length / 100),
        api_response_summary_last_page_processed: Math.ceil(
          records.length / 100
        ),
        processing_summary_records_processed: successCount + errorCount,
        processing_summary_records_created: createdCount,
        processing_summary_records_updated: updatedCount,
        processing_summary_records_failed: errorCount,
        processing_summary_shops_processed: shopsProcessed,
        processing_summary_dinings_processed: diningsProcessed,
        // Note: validation_issues and errors are handled via separate relation tables
        performance_metrics_avg_time_per_record: avgTimePerRecord,
        performance_metrics_memory_usage_mb: Math.round(memoryUsage),
        performance_metrics_api_response_time_avg: avgApiResponseTime,
        notes: `Sync completed with ${successCount} successful, ${errorCount} failed records.`,
      },
    });

    console.log("\n=== Sync Summary ===");
    console.log(`✅ Successfully synced: ${successCount} records`);
    console.log(`📝 Created: ${createdCount} records`);
    console.log(`🔄 Updated: ${updatedCount} records`);
    console.log(`❌ Errors: ${errorCount} records`);
    console.log(`📊 Total records processed: ${records.length}`);
    console.log(`🏪 Shops processed: ${shopsProcessed}`);
    console.log(`🍽️  Dinings processed: ${diningsProcessed}`);
    console.log(`⚡ Average time per record: ${avgTimePerRecord}ms`);
    console.log(`🌐 Average API response time: ${avgApiResponseTime}ms`);
    console.log(`💾 Memory usage: ${Math.round(memoryUsage)}MB`);

    if (errors.length > 0) {
      console.log("\n=== Errors ===");
      errors.forEach((error) => {
        console.log(
          `   Record ${error.record_unique_id}: ${error.error_message}`
        );
      });
    }

    if (validationIssues.length > 0) {
      console.log("\n=== Validation Issues ===");
      validationIssues.forEach((issue) => {
        console.log(
          `   Record ${issue.record_unique_id}: ${issue.description}`
        );
      });
    }

    if (unmappedFloors.size > 0) {
      console.log("\n=== Unmapped Floors ===");
      Array.from(unmappedFloors).forEach((floor) => {
        console.log(`   - "${floor}"`);
      });
    }

    if (unmappedCategories.size > 0) {
      console.log("\n=== Unmapped Categories ===");
      Array.from(unmappedCategories).forEach((cat) => {
        const categoryInfo = JSON.parse(cat);
        console.log(
          `   - "${categoryInfo.category_name}" (${categoryInfo.type})`
        );
      });
    }
  } catch (error) {
    console.error("Sync failed:", error);

    // Update sync log with error
    if (syncLog) {
      await prisma.api_sync_logs.update({
        where: { id: syncLog.id },
        data: {
          status: "FAILED",
          completed_at: new Date(),
          // Note: errors are handled via separate relation table
          notes: "Sync failed due to critical error.",
        },
      });
    }

    throw error;
  } finally {
    await prisma.$disconnect();
  }
}

// Handle graceful shutdown
process.on("SIGINT", async () => {
  console.log("\n🛑 Received SIGINT, shutting down gracefully...");
  await prisma.$disconnect();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.log("\n🛑 Received SIGTERM, shutting down gracefully...");
  await prisma.$disconnect();
  process.exit(0);
});

// Start the sync process
syncExternalApi()
  .then(() => {
    console.log("✅ Sync completed successfully");
    process.exit(0);
  })
  .catch((error) => {
    console.error("❌ Sync failed:", error);
    process.exit(1);
  });
